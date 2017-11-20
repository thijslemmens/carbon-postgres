package eu.thijslemmens.carbonpostgres

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.stream.scaladsl.SourceQueueWithComplete

/**
  * The Source.queue offer method is not threadsafe. This actor can be used to forward a message to the queue in a
  * threadsafe wayl
  *
  * @param queue The queue to proxy
  */
class SourceQueueProxy(val queue: SourceQueueWithComplete[Record]) extends Actor with ActorLogging{

  override def receive = {
    case message: Record => {
      log.debug("Offering new record")
      val s = sender()
      queue.offer(message).onComplete(tQOR => s ! {
        log.debug("Replying to sender")
        tQOR.get
      })(context.dispatcher)
    }
    case  _ => sender() ! "Message of the wrong type!"
  }

}
