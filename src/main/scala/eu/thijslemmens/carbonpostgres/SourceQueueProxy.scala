package eu.thijslemmens.carbonpostgres

import akka.actor.{Actor, ActorLogging, ActorRef, Stash}
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.pattern.pipe


/**
  * The Source.queue offer method is not threadsafe. This actor can be used to forward a message to the queue in a
  * threadsafe way
  *
  * @param queue The queue to proxy
  */
class SourceQueueProxy(val queue: SourceQueueWithComplete[Record]) extends Actor with ActorLogging with Stash{

  import context._

  override def receive = {
    case record: Record => {
      active(record)
    }
    case  _ => sender() ! "Message of the wrong type!"
  }

  def active: Receive = {
    case record: Record => {
      val future = queue.offer(record)
      log.debug("Piping future to sender "+record)
      future pipeTo sender()
      log.debug("Becoming inactive")
      become(inactive)
      future.onComplete(tQOR => {
        log.debug("Sending Activate message")
        context.self ! Activate
      })(context.dispatcher)
    }
    case  _ => sender() ! "Message of the wrong type!"

  }

  def inactive: Receive = {
    case record: Record => {
      log.debug("Stashing record "+record)
      stash()
    }
    case Activate => {
      log.debug("Activating")
      unstashAll()
      become(active)
    }
    case  _ => sender() ! "Message of the wrong type!"
  }

  case class Activate()

}
