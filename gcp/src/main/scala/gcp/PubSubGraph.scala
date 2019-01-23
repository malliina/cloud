package gcp

import java.util.concurrent.{Semaphore, TimeUnit}

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.google.api.core.ApiService
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.common.util.concurrent.MoreExecutors
import com.google.pubsub.v1.PubsubMessage
import gcp.PubSubGraph.log
import org.slf4j.LoggerFactory

import scala.collection.mutable

object PubSubGraph {
  private val log = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  def apply(settings: PubSubSettings) = new PubSubGraph(settings)
}

class PubSubGraph(settings: PubSubSettings) extends GraphStage[SourceShape[FullMessage]] {
  val out = Outlet[FullMessage]("FullMessage.out")
  override val shape = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    val queue = mutable.Queue[FullMessage]()
    val backpressure = new Semaphore(settings.bufferSize)

    val logic: GraphStageLogic = new GraphStageLogic(shape) {
      val onConnect = getAsyncCallback[Subscriber] { _ =>
        }
      val onMessage = getAsyncCallback[FullMessage] { msg =>
        if (isAvailable(out)) pushMessage(msg)
        else queue.enqueue(msg)
      }
      val onConnectionLost = getAsyncCallback[Throwable] { ex =>
        failStage(ex)
      }
      val receiver: MessageReceiver = new MessageReceiver {
        override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
          backpressure.acquire()
          onMessage.invoke(FullMessage(message, consumer))
        }
      }
      val subscriber = Subscriber
        .newBuilder(settings.subscription, receiver)
        .setCredentialsProvider(settings.creds)
        .build()
      subscriber.addListener(
        new ApiService.Listener {
          override def running(): Unit = {
            log.info(s"Connected to $describe.")
            onConnect.invoke(subscriber)
          }

          override def terminated(from: ApiService.State): Unit = {
            log.info(s"Closed connection to $describe.")
          }

          override def failed(from: ApiService.State, failure: Throwable): Unit = {
            log.error(s"Connection failed to $describe.", failure)
            onConnectionLost.invoke(failure)
          }
        },
        MoreExecutors.directExecutor()
      )

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          if (queue.nonEmpty)
            pushMessage(queue.dequeue())
        }
      })

      override def preStart(): Unit = {
        log.info(s"Connecting to $describe...")
        subscriber.startAsync()
      }

      override def postStop(): Unit = {
        try {
          log.info(s"Disconnecting from $describe...")
          subscriber.stopAsync().awaitTerminated(5, TimeUnit.SECONDS)
        } catch {
          case _: Exception =>
            ()
        }
      }

      def pushMessage(message: FullMessage): Unit = {
        push(out, message)
        backpressure.release()
      }

      def describe = s"topic '${settings.topic}' with subscription '${settings.subscription.getSubscription}'"
    }
    logic
  }
}
