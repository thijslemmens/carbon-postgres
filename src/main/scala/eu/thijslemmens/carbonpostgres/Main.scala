package eu.thijslemmens.carbonpostgres

import akka.actor.ActorSystem
import akka.event.Logging
import akka.pattern._
import akka.stream._
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.stream.scaladsl.{Framing, _}
import akka.util.{ByteString, Timeout}
import eu.thijslemmens.carbonpostgres.carbon.CarbonConnector
import eu.thijslemmens.carbonpostgres.config._
import eu.thijslemmens.carbonpostgres.db.{DbQueueFactory, PostgresWriter}

import scala.concurrent._
import scala.concurrent.duration._



object Main extends App{
  implicit val system = ActorSystem("CarbonImpersonator")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher
  val log = Logging(system, "MainLogger")

  private val MEGABYTE = 1024L * 1024L

  def bytesToMeg(bytes: Long) = bytes / MEGABYTE
  val heapMaxSize = bytesToMeg(Runtime.getRuntime.maxMemory)
  log.info(s"Starting application with max heap size $heapMaxSize Mb")


  val configProvider: ConfigProvider = new CascadingConfigProvider(new SystemPropConfigProvider, new EnvConfigProvider, new DefaultConfigProvider)

  val postgresWriter = new PostgresWriter(configProvider.getStringParameter("db.host").get,
    configProvider.getIntParameter("db.port").get,
    configProvider.getStringParameter("db.user").get,
    configProvider.getStringParameter("db.password"),
    configProvider.getIntParameter("db.poolsize").get,
    configProvider.getStringParameter("db.database"))

  val dbQueueFactory: DbQueueFactory = new DbQueueFactory(postgresWriter, 10, 2000, 1000)

  log.info("Starting DBQueue")
  val dbQueue = dbQueueFactory.getDbQueue()

  val carbonConnector = new CarbonConnector(dbQueue, configProvider.getStringParameter("carbon.tcp.host").get,
    configProvider.getIntParameter("carbon.tcp.port").get)

  carbonConnector.start()

  sys.ShutdownHookThread {
    log.info("Shutting Down the dbQueue")
    Await.result(dbQueue.shutDown(), 10.seconds)
    log.info("DbQueue shut down")
    system.terminate()
  }

  }


