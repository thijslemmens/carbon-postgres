package eu.thijslemmens.carbonpostgres
import akka.Done

import scala.concurrent.{ExecutionContext, Future}

trait DbWriter {

  def write(record: Record): Future[Done]

  def write(records: List[Record]): Future[Done]
}
