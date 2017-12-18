package eu.thijslemmens.carbonpostgres.db

import akka.Done
import akka.actor.ActorSystem
import akka.event.Logging
import com.github.mauricio.async.db.Configuration
import com.github.mauricio.async.db.pool.{ConnectionPool, PoolConfiguration}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.pool.PostgreSQLConnectionFactory
import eu.thijslemmens.carbonpostgres.Record

import scala.concurrent.Future

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
