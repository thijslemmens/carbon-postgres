package eu.thijslemmens.carbonpostgres.db

import akka.actor.Props
import akka.pattern._
import akka.stream.QueueOfferResult
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.util.Timeout
import eu.thijslemmens.carbonpostgres.{BaseTest, Record}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class SourceQueueProxySpec extends BaseTest {

  implicit override val timeout = Timeout(100.seconds)

  "A SourceQueueProxy must" must {
    "Pass records sequentially to a Source.queue" in {
      val sourceQueue = mock[SourceQueueWithComplete[Record]]
      val actor = system.actorOf(Props(new SourceQueueProxy(sourceQueue)), "SourceQueueProxy")

      val record1 = Record(1, "test", 12)
      val record2 = Record(2, "test", 13)
      within(1.second){
        inSequence {
          (sourceQueue.offer(_)).expects(record1).returns({
            Future {
              Thread.sleep(100)
              QueueOfferResult.enqueued
            }
          })

          (sourceQueue.offer(_)).expects(record2).returns({
            Future {
              Thread.sleep(100)
              QueueOfferResult.enqueued
            }
          })

          val f1 = actor ? record1
          val f2 = actor ? record2

          Await.result(f1, 200.millis)
          Await.result(f2, 200.millis)
          Thread.sleep(500)
        }
      }
    }
  }

  class DummyDbQueue extends DbQueue {
    override def queue(record: Record): Future[QueueOfferResult] = {
      Future {
        Thread.sleep(100)
        QueueOfferResult.enqueued
      }
    }
  }

}
