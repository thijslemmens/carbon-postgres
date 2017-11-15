package eu.thijslemmens.carbonpostgres

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.concurrent.duration._


@RunWith(classOf[JUnitRunner])
class DbQueueSpec extends TestKit(ActorSystem("testActorSystem")) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll with MockFactory{

  implicit val ec: ExecutionContext = system.dispatcher
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

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
      val record1 = new Record(1234567, "test.metric", 1234)
      val record2 = new Record(12345678, "test.metric", 12345)
      val record3 = new Record(123456789, "test.metric", 123456)

      inSequence {
        (dbWriter.write(_: List[Record])).expects(List(record1)).returns({
          Future {
            blocking {
              Thread.sleep(1000)
              Done
            }
          }
        })

        (dbWriter.write(_: List[Record])).expects(List(record1, record2)).returns(Future.successful(Done))

        dbQueue.queue(record1)
        dbQueue.queue(record2)
        dbQueue.queue(record3)
        Thread.sleep(2000)
      }

    }
  }

}
