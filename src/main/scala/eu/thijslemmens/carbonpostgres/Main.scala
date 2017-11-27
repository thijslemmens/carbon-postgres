package eu.thijslemmens.carbonpostgres

import akka.actor.ActorSystem
import akka.event.Logging
import akka.pattern._
import akka.stream._
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.stream.scaladsl.{Framing, _}
import akka.util.{ByteString, Timeout}
import eu.thijslemmens.carbonpostgres.db.{DbQueueFactory, PostgresWriter}

import scala.concurrent._



object Main extends App{

  implicit val system = ActorSystem("CarbonImpersonator")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  val log = Logging(system, "MainLogger");

  val postgresWriter = new PostgresWriter("localhost", 3456, "timescale", Some("timescale"), 10, Some("timescale"))

  val dbQueueFactory: DbQueueFactory = new DbQueueFactory(postgresWriter, 10)

  val dbQueue = dbQueueFactory.getDbQueue()

  val connections: Source[IncomingConnection, Future[ServerBinding]] =
    Tcp().bind("127.0.0.1", 2003)
  connections runForeach { connection =>
    log.debug(s"New connection from: ${connection.remoteAddress}")

    val tcpToRecord = Flow[ByteString]
      .via(Framing.delimiter(
        ByteString("\n"),
        maximumFrameLength = 256,
        allowTruncation = true))
      .map(_.utf8String)
      .map(str => {
        val record = stringToRecord(str)
        log.debug(record.toString)
        record
      })
      .mapAsync(parallelism = 10)(record => {
        log.debug("Sending message to queue")
        dbQueue.queue(record)
      })
      .map( queueOfferResult => {
        if(queueOfferResult == QueueOfferResult.enqueued){
          ByteString("Enqueued")
        } else {
          ByteString("Something went wrong")
        }
      })

    connection.handleWith(tcpToRecord)
  }


  def stringToRecord(str: String): Record = {
    val split = str.split(" ")
    Record(split(2).toLong, split(0), split(1).toFloat)
  }

}


