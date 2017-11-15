package eu.thijslemmens.carbonpostgres

import akka.stream.QueueOfferResult

import scala.concurrent.Future

trait DbQueue {

  def queue(record: Record): Future[QueueOfferResult]

}
