package eu.thijslemmens.carbonpostgres

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object TestPostgresWriter extends App {

  implicit val system = ActorSystem("QuickStart")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  val postgresWriter: PostgresWriter = new PostgresWriter(
    "localhost",
    3456,
    "postgres",
    None,
    4,
    Some("testtimescale")
  )

  val done = postgresWriter.write(Record(12345, "test.test", 6))
  done.onComplete {
    case Success(Done) => {
      System.out.println("Record succesfully saved")
      system.terminate()
    }
    case Failure(exception) => {
      System.out.println(exception)
      system.terminate()
    }
  }


}
