package eu.thijslemmens.carbonpostgres

import akka.actor.Actor
import akka.stream.scaladsl.SourceQueueWithComplete

/**
  * The Source.queue offer method is not threadsafe. This actor can be used to forward a message to the queue in a
  * threadsafe wayl
  *
  * @param queue The queue to proxy
  * @tparam T The type of message the queue takes
  */
class SourceQueueProxy[T](val queue: SourceQueueWithComplete[T]) extends Actor {

  override def receive = {
    case message: T => {
      sender() ! queue.offer(message)
    }
    case  _ => sender() ! "Message of the wrong type!"
  }

}
