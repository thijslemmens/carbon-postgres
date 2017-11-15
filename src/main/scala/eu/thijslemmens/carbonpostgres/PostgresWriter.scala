package eu.thijslemmens.carbonpostgres

import akka.Done
import akka.actor.ActorSystem
import akka.event.Logging
import com.github.mauricio.async.db.{Configuration, Connection, QueryResult}
import com.github.mauricio.async.db.pool.{ConnectionPool, PoolConfiguration}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.pool.PostgreSQLConnectionFactory

import scala.concurrent.{ExecutionContext, Future}

class PostgresWriter(val host: String, val port: Int, val user: String, val password: Option[String], val poolSize: Int, val database: Option[String])(implicit val system: ActorSystem) extends DbWriter {

  val log = Logging(system, "PostgresWriter")
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
