package eu.thijslemmens.carbonpostgres.db

import akka.Done
import eu.thijslemmens.carbonpostgres.Record

import scala.concurrent.Future

trait DbWriter {

  def write(record: Record): Future[Done]

  def write(records: List[Record]): Future[Done]
}
