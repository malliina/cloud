package gcp

import com.google.api.gax.core.CredentialsProvider
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage}

case class PubSubSettings(topic: PubSubTopic,
                          subscription: ProjectSubscriptionName,
                          creds: CredentialsProvider = GCP.credentialsProvider,
                          bufferSize: Int = 1000)

case class FullMessage(content: PubsubMessage, reply: AckReplyConsumer) {
  def message: String = content.getData.toStringUtf8

  def ack(): Unit = reply.ack()

  def nack(): Unit = reply.nack()
}
