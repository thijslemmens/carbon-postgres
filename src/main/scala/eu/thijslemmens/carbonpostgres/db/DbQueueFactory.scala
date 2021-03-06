package eu.thijslemmens.carbonpostgres.db

import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import akka.pattern._
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream._
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Merge, Sink, Source}
import akka.util.Timeout
import akka.{Done, NotUsed}
import eu.thijslemmens.carbonpostgres.Record

import scala.concurrent.Future
import scala.concurrent.duration._

class DbQueueFactory(val dbWriter: DbWriter, val parallellism: Int, val maxQueueSize: Int = 10, val maxBatchSize: Int = 1000)(implicit val system: ActorSystem) {

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
    val batch: Flow[Record, List[Record], NotUsed] = Flow[Record].batch(maxBatchSize, record => {
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

    return new DbQueue {
      override def shutDown(): Future[Done] = {
        dbQueue.complete()
        dbQueue.watchCompletion()
      }

      override def queue(record: Record): Future[QueueOfferResult] = {
        val result = actor ? record
        result.asInstanceOf[Future[QueueOfferResult]]
      }
    }
  }

}
