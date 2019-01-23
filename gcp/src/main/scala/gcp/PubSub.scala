package gcp

import java.util.concurrent.TimeUnit

import com.google.api.core.{ApiFutureCallback, ApiFutures}
import com.google.api.gax.batching.BatchingSettings
import com.google.cloud.pubsub.v1._
import com.google.common.util.concurrent.MoreExecutors
import com.google.protobuf.ByteString
import com.google.pubsub.v1._
import gcp.GCP.credentialsProvider

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.concurrent.{Future, Promise}

case class PubSubTopic(topic: String) {
  override def toString = topic
}

object PubSubTopic {
  def random() = PubSubTopic(Utils.randomString(6))
}

case class MessageId(id: String)

case class SubscriptionId(id: String)

object SubscriptionId {
  def random() = SubscriptionId(Utils.randomString(6))
}

object PubSub {
  def apply(projectId: String) = new PubSub(projectId)

  def using[Res <: AutoCloseable, T](res: Res)(code: Res => T): T =
    try code(res)
    finally res.close()
}

class PubSub(projectId: String) extends AutoCloseable {
  val projectName = ProjectName.of(projectId)
  val project = ProjectName.format(projectId)

  val topicSettings = TopicAdminSettings.newBuilder().setCredentialsProvider(credentialsProvider).build()
  val topicClient = TopicAdminClient.create(topicSettings)
  val subSettings = SubscriptionAdminSettings.newBuilder().setCredentialsProvider(credentialsProvider).build()
  val subClient = SubscriptionAdminClient.create(subSettings)

  def topics: List[Topic] =
    topicClient.listTopics(project).iterateAll().asScala.toList

  def createTopic(topic: PubSubTopic): Topic =
    topicClient.createTopic(ProjectTopicName.of(projectId, topic.topic))

  def deleteTopic(topic: PubSubTopic): Unit =
    topicClient.deleteTopic(ProjectTopicName.format(projectId, topic.topic))

  def subscriptions: List[Subscription] = {
    val req = ListSubscriptionsRequest.newBuilder().setProject(project).build()
    subClient.listSubscriptions(req).iterateAll().asScala.toList
  }

  def createSubscription(topic: PubSubTopic, id: SubscriptionId): Subscription = {
    val subName = ProjectSubscriptionName.of(projectId, id.id)
    val projectTopic = ProjectTopicName.of(projectId, topic.topic)
    subClient.createSubscription(subName, projectTopic, PushConfig.getDefaultInstance, 0)
  }

  def topicSubscriptions(topic: PubSubTopic): List[String] = {
    val projectTopic = ProjectTopicName.of(projectId, topic.topic)
    val req = ListTopicSubscriptionsRequest.newBuilder().setTopic(projectTopic.toString).build()
    topicClient.listTopicSubscriptions(req).iterateAll().asScala.toList
  }

  def deleteSubscription(id: SubscriptionId) = {
    val name = ProjectSubscriptionName.of(projectId, id.id)
    subClient.deleteSubscription(name)
    name
  }

  def publish(message: String, topic: PubSubTopic): Future[MessageId] = {
    val noBatching = BatchingSettings.newBuilder().setIsEnabled(false).build()
    val publisher = Publisher
      .newBuilder(ProjectTopicName.of(projectId, topic.topic))
      .setCredentialsProvider(credentialsProvider)
      .setBatchingSettings(noBatching)
      .build()
    try {
      val msg = ByteString.copyFromUtf8(message)
      val pubSubMessage = PubsubMessage.newBuilder().setData(msg).build()
      val res = publisher.publish(pubSubMessage)
      val promise = Promise[MessageId]()
      ApiFutures.addCallback(
        res,
        new ApiFutureCallback[String] {
          override def onFailure(t: Throwable): Unit = promise.tryFailure(t)

          override def onSuccess(result: String): Unit = promise.trySuccess(MessageId(result))
        },
        MoreExecutors.directExecutor()
      )
      promise.future
    } finally {
      publisher.shutdown()
      publisher.awaitTermination(5, TimeUnit.SECONDS)
    }
  }

  def receive(subscription: SubscriptionId)(handle: PubsubMessage => Boolean): Subscriber = {
    val receiver: MessageReceiver = new MessageReceiver {
      override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
        val handled = handle(message)
        if (handled) consumer.ack() else consumer.nack()
      }
    }
    val subName = ProjectSubscriptionName.of(projectId, subscription.id)
    Subscriber.newBuilder(subName, receiver).setCredentialsProvider(credentialsProvider).build()
  }

  def close(): Unit = {
    subClient.close()
    topicClient.close()
  }
}
