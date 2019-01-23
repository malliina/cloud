package gcp

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.google.api.core.ApiService
import com.google.common.util.concurrent.MoreExecutors
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage}
import org.scalatest.FunSuite

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}

class PubSubTests extends FunSuite {
  val projectId = "random-226820"

  ignore("PubSub using Akka Streams") {
    implicit val as = ActorSystem("test")
    implicit val mat = ActorMaterializer()

    PubSub.using(PubSub(projectId)) { client =>
      val testTopic = PubSubTopic.random()
      val _ = client.createTopic(testTopic)
      val testSub = SubscriptionId.random()
      val __ = client.createSubscription(testTopic, testSub)

      val testSubName = ProjectSubscriptionName.of(projectId, testSub.id)
      val src = PubSubSource(PubSubSettings(testTopic, testSubName))
      val running = src.take(1).runWith(Sink.seq[FullMessage])
      val testMessage = "Hello"
      client.publish(testMessage, testTopic)
      val messages = await(running)
      assert(messages.map(_.message).contains(testMessage))
      //      println(messages)
      messages.foreach(_.reply.ack())
      client.deleteSubscription(testSub)
      client.deleteTopic(testTopic)
    }
  }

  ignore("PubSub") {
    PubSub.using(PubSub(projectId)) { client =>
      val testTopic = PubSubTopic.random()
      val _ = client.createTopic(testTopic)
      client.topics.map(_.getName) foreach println
      val testSub = SubscriptionId.random()
      val __ = client.createSubscription(testTopic, testSub)
      val p = Promise[PubsubMessage]()
      val subscriber = client.receive(testSub) { message =>
        p.trySuccess(message)
        true
      }
      subscriber.addListener(
        new ApiService.Listener {
          override def failed(from: ApiService.State, failure: Throwable): Unit = {
            println(s"Errored with $failure")
          }
        },
        MoreExecutors.directExecutor()
      )
      subscriber.startAsync()
      val testMessage = "Do it"
      val sentMessageId = await(client.publish(testMessage, testTopic))
      val receivedMessage = await(p.future)
      assert(MessageId(receivedMessage.getMessageId) === sentMessageId)
      assert(receivedMessage.getData.toStringUtf8 == testMessage)
      subscriber.stopAsync().awaitTerminated()
      client.deleteSubscription(testSub)
      client.deleteTopic(testTopic)
    }
  }

  def await[T](f: Future[T]): T = Await.result(f, 20.seconds)
}
