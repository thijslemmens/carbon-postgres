package eu.thijslemmens.carbonpostgres

import akka.Done
import akka.pattern._
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.event.Logging
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult, Supervision}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import scala.concurrent.duration._


import scala.concurrent.Future

class DbQueueFactory(val dbWriter: DbWriter, val parallillism: Int)(implicit val system: ActorSystem) {

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val timeout = Timeout(5.seconds)

  val log = Logging(system, "DbQueueFactory")

  val decider: Supervision.Decider = {
    case e: Throwable => {
      log.error("An error occurred while writing to postgres", e)
      Supervision.resume
    }
  }

  def getDbQueue(): DbQueue = {
    val dbQueue = Source.queue[Record](10, OverflowStrategy.backpressure)
      .batch(1, record => {
        List(record)
      })( (list, record) => {
        list :+ record
      })
      .mapAsync(parallillism)(records => {
      val done = dbWriter.write(records)
      log.debug("written to pg")
      done
    }).withAttributes(supervisionStrategy(decider))
      .to(Sink.foreach[Done]( _ => {
        log.debug("record succesfully added")
      })).run()

    val actor = system.actorOf(Props(new SourceQueueProxy[Record](dbQueue)))

    return (record: Record) => {
      val result = actor ? record
      result.asInstanceOf[Future[Future[QueueOfferResult]]].flatten
    }
  }

}
