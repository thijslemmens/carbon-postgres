package eu.thijslemmens.carbonpostgres.db

import akka.Done
import akka.actor.ActorSystem
import akka.event.Logging
import com.github.mauricio.async.db.Configuration
import com.github.mauricio.async.db.pool.{ConnectionPool, PoolConfiguration}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.pool.PostgreSQLConnectionFactory
import eu.thijslemmens.carbonpostgres.Record

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.util.control.Breaks._



class PostgresWriter(val host: String, val port: Int, val user: String, val password: Option[String], val poolSize: Int, val database: Option[String])(implicit val system: ActorSystem) extends DbWriter {
  val log = Logging(system, "PostgresWriter")
  log.info(s"Initializing Postgreswriter with host: $host, port: $port, database: $database, user: $user, password: $password, poolSize: $poolSize")
  implicit val ec = system.dispatcher

  private val configuration: Configuration = new Configuration(
    username = user,
    password = password,
    host = host,
    port = port,
    database = database)

  private val connection: ConnectionPool[PostgreSQLConnection] =
    new ConnectionPool[PostgreSQLConnection](
      new PostgreSQLConnectionFactory(configuration),
      PoolConfiguration.Default
    )

  initialize()


  private def initialize(): Unit = {
    var loop = true
    var i = 0
    while(loop && i < 5){
      i += 1
      try {
        Await.result(connection.sendQuery("SELECT 1"), 1.second)
        loop = false
      } catch {
        case e: Exception => {
          log.warning("Validation failed")
          Thread.sleep(1000)
        }
      }
    }
    val result = connection.sendQuery("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;").andThen {
        case Success(_) => connection.sendQuery("ALTER EXTENSION timescaledb UPDATE;")
//        case Failure(e) => log.error("Something went wrong", e)
      } andThen {
      case Success(_) => connection.sendQuery("CREATE TABLE records (" +
        "  time TIMESTAMP NOT NULL ," +
        "  metric TEXT NOT NULL ," +
        "  VALUE FLOAT NOT NULL" +
        ");");
//      case Failure(e) => log.error("Something went wrong", e)
    } andThen {
      case Success(_) => connection.sendQuery("SELECT create_hypertable('records', 'time', 'metric', 4);")
//      case Failure(e) => log.error("Something went wrong", e)
    }

    Await.result(result, 2.seconds)
    log.info("Database is initialized")
  }

  override def write(record: Record): Future[Done] = {
    val futureQueryResult = connection.sendPreparedStatement("INSERT INTO records(time, metric, value) VALUES (to_timestamp(?),?,?)", Array(record.timeStamp, record.key, record.value))
    futureQueryResult.map( queryResult => Done)
  }

  override def write(records: List[Record]): Future[Done] = {
    log.info(s"Writing batch of ${records.size} records")
    val baseQuery = "INSERT INTO records(time, metric, value) VALUES ";
    val values = records.map(record => {
      s"(to_timestamp(${record.timeStamp}), '${record.key}', ${record.value})"
    }).mkString(",");
    val query = baseQuery + values
    log.debug(query)
    val futureQueryResult = connection.sendPreparedStatement(query);
    futureQueryResult.map( queryResult => Done)
  }

}
