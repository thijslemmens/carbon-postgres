package eu.thijslemmens.carbonpostgres

import akka.{Done, NotUsed}
import akka.pattern._
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.event.Logging
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream._
import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Merge, Sink, Source}
import akka.util.Timeout
import GraphDSL.Implicits._


import scala.concurrent.duration._
import scala.concurrent.Future

class DbQueueFactory(val dbWriter: DbWriter, val parallellism: Int, val maxQueueSize: Int = 10)(implicit val system: ActorSystem) {

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
    val batch: Flow[Record, List[Record], NotUsed] = Flow[Record].batch(1000, record => {
      List(record)
    })( (list, record) => {
      list :+ record
    })

    def batchParallel(workers: Int): Flow[Record, List[Record], NotUsed] =
      Flow.fromGraph(GraphDSL.create() { implicit builder =>
        val dispatch = builder.add(Balance[Record](workers, waitForAllDownstreams = true))
        val merge = builder.add(Merge[List[Record]](workers))

        for(_ <- 1 to workers){
          dispatch ~> batch ~> merge
        }
        FlowShape(dispatch.in, merge.out)
    })

    val dbQueue = Source.queue[Record](maxQueueSize, OverflowStrategy.backpressure)
      .via(batchParallel(parallellism))
//        .via(batch)
      .mapAsync(parallellism)(records => {
      val done = dbWriter.write(records)
      log.debug("written to pg")
      done
    }).withAttributes(supervisionStrategy(decider))
      .to(Sink.foreach[Done]( _ => {
        log.debug("record succesfully added")
      })).run()

    val actor = system.actorOf(Props(new SourceQueueProxy(dbQueue)))

    return (record: Record) => {
      val result = actor ? record
      result.asInstanceOf[Future[QueueOfferResult]]
    }
  }

}
