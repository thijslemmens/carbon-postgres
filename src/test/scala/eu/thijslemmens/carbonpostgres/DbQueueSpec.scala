package eu.thijslemmens.carbonpostgres

import akka.Done
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, QueueOfferResult}
import akka.testkit.TestKit
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class DbQueueSpec extends BaseTest {

  val dbWriter = mock[DbWriter]
  val dbQueueFactory: DbQueueFactory = new DbQueueFactory(dbWriter, 1)
  val dbQueue: DbQueue = dbQueueFactory.getDbQueue()

  "A DbQueue" must{
    "write to a db when offering one record" in {
      val record1 = new Record(1234567, "test.metric", 1234)

      (dbWriter.write(_: List[Record])).expects(List(record1)).returns(Future.successful(Done))
      dbQueue.queue(record1)
      Thread.sleep(100)
    }

    "batch when writing to a slow db" in {
      val dbWriter2 = new DummyDbWriter
      val dbQueueFactory2: DbQueueFactory = new DbQueueFactory(dbWriter2, 10)
      val dbQueue2: DbQueue = dbQueueFactory2.getDbQueue()

      val record1 = new Record(1234567, "test.metric", 1234)
      val record2 = new Record(12345678, "test.metric", 12345)
      val record3 = new Record(123456789, "test.metric", 123456)
      val nmbOfRecords = 100000

      val testStream = Source(1 to nmbOfRecords).mapAsync(10){ i => {
        dbQueue2.queue(record1.copy(value = i))
      }
      }.runWith(Sink.foreach[QueueOfferResult](qOR => assert(qOR === QueueOfferResult.enqueued)))

      Await.result(testStream, 100.seconds)

      while (dbWriter2.nmbOfRecords < nmbOfRecords){
        Thread.sleep(1000)
      }
      assert(dbWriter2.multiRecords)
      testStream

    }
  }

  class DummyDbWriter extends DbWriter {
    override def write(record: Record) = {
      Future.successful(Done)
    }

    var multiRecords = false
    var nmbOfRecords = 0
    override def write(records: List[Record]) = {
      if(records.length > 1){
        multiRecords = true
      }
      nmbOfRecords +=  records.length
      Future {
        Thread.sleep(100)
        Done
      }
    }
  }

}