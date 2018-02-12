package eu.thijslemmens.carbonpostgres.carbon

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.{ActorMaterializer, QueueOfferResult}
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.stream.scaladsl.{Flow, Framing, Source, Tcp}
import akka.util.ByteString
import eu.thijslemmens.carbonpostgres.db.DbQueue
import eu.thijslemmens.carbonpostgres.{InputConnector, Record}

import scala.concurrent.Future

class CarbonConnector(val dbQueue: DbQueue, val host: String, val port: Int)(implicit val system: ActorSystem) extends InputConnector {

  implicit val materializer: ActorMaterializer = ActorMaterializer()


  val log = Logging(system, "CarbonConnector")

  override def start(): Unit = {
    val connections: Source[IncomingConnection, Future[ServerBinding]] =
      Tcp().bind(host, port)

    log.info("Starting to listen for tcp connections")
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
  }

  private def stringToRecord(str: String): Record = {
    val split = str.split(" ")
    Record(split(2).toLong, split(0), split(1).toFloat)
  }
}
