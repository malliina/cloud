package gcp

import akka.NotUsed
import akka.stream.scaladsl.Source

object PubSubSource {
  def apply(settings: PubSubSettings): Source[FullMessage, NotUsed] = {
    val graph = PubSubGraph(settings)
    Source.fromGraph(graph)
  }
}
