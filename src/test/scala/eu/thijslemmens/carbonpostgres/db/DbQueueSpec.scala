package eu.thijslemmens.carbonpostgres.db

import akka.Done
import akka.event.Logging
import akka.stream.QueueOfferResult
import akka.stream.scaladsl.{Sink, Source}
import eu.thijslemmens.carbonpostgres.{BaseTest, Record}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

@RunWith(classOf[JUnitRunner])
class DbQueueSpec extends BaseTest {

  val dbWriter = mock[DbWriter]
  val dbQueueFactory: DbQueueFactory = new DbQueueFactory(dbWriter, 1)
  val dbQueue: DbQueue = dbQueueFactory.getDbQueue()
  val log = Logging(system, "DbQueueSpec")

  "A DbQueue" must{
    "write to a db when offering one record" in {
      val record1 = new Record(1234567, "test.metric", 1234)

      (dbWriter.write(_: List[Record])).expects(List(record1)).returns(Future.successful(Done))
      dbQueue.queue(record1)
      Thread.sleep(100)
    }

    "batch when writing to a slow db" in {
      val dbWriter2 = new DummyDbWriter
      val dbQueueFactory2: DbQueueFactory = new DbQueueFactory(dbWriter2, 2)
      val dbQueue2: DbQueue = dbQueueFactory2.getDbQueue()

      val record1 = new Record(1234567, "test.metric", 1234)
      val nmbOfRecords = 1000

      val testStream = Source(1 to nmbOfRecords).mapAsync(2){ i => {
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
      log.debug(s"WRITING ${records.length} RECORDS")
      if(records.length > 1){
        multiRecords = true
      }
      nmbOfRecords +=  records.length
      Future {
        Thread.sleep(100)
        log.debug("RESOLVING FUTURE")
        Done
      }
    }
  }

}