package eu.thijslemmens.carbonpostgres

import akka.actor.ActorSystem
import akka.event.Logging
import akka.pattern._
import akka.stream._
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.stream.scaladsl.{Framing, _}
import akka.util.{ByteString, Timeout}
import eu.thijslemmens.carbonpostgres.config.{CascadingConfigProvider, ConfigProvider, DefaultConfigProvider}
import eu.thijslemmens.carbonpostgres.db.{DbQueueFactory, PostgresWriter}

import scala.concurrent._
import scala.concurrent.duration._



object Main extends App{

  implicit val system = ActorSystem("CarbonImpersonator")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  val log = Logging(system, "MainLogger")

  val configProvider: ConfigProvider = new CascadingConfigProvider(new EnvConfigProvider, new DefaultConfigProvider)

  val postgresWriter = new PostgresWriter(configProvider.getStringParameter("db.host").get,
    configProvider.getIntParameter("db.port").get,
    configProvider.getStringParameter("db.user").get,
    configProvider.getStringParameter("db.password"),
    configProvider.getIntParameter("db.poolsize").get,
    configProvider.getStringParameter("db.database"))

  val dbQueueFactory: DbQueueFactory = new DbQueueFactory(postgresWriter, 10, 2000, 1000)

  val dbQueue = dbQueueFactory.getDbQueue()

  val connections: Source[IncomingConnection, Future[ServerBinding]] =
    Tcp().bind(configProvider.getStringParameter("carbon.tcp.host").get,
      configProvider.getIntParameter("carbon.tcp.port").get)
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

  sys.ShutdownHookThread {
    log.info("Shutting Down the dbQueue")
    Await.result(dbQueue.shutDown(), 10.seconds)
    log.info("DbQueue shut down")
    system.terminate()
  }


  def stringToRecord(str: String): Record = {
    val split = str.split(" ")
    Record(split(2).toLong, split(0), split(1).toFloat)
  }

}


