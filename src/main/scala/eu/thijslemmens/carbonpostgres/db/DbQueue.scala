package eu.thijslemmens.carbonpostgres.db

import akka.stream.QueueOfferResult
import eu.thijslemmens.carbonpostgres.Record

import scala.concurrent.Future

trait DbQueue {

  def queue(record: Record): Future[QueueOfferResult]

}
