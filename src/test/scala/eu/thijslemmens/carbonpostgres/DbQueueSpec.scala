package eu.thijslemmens.carbonpostgres

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.concurrent.Future
import scala.concurrent.duration._


@RunWith(classOf[JUnitRunner])
class DbQueueSpec extends TestKit(ActorSystem("testActorSystem")) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll with MockFactory{

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "A DbQueue" must{
    "write to a db when offering one record" in {
      val dbWriter = mock[DbWriter]
      val dbQueueFactory: DbQueueFactory = new DbQueueFactory(dbWriter, 1)
      val dbQueue: DbQueue = dbQueueFactory.getDbQueue()

      val record1 = new Record(1234567, "test.metric", 1234)

      (dbWriter.write(_: List[Record])).expects(List(record1)).returns(Future.successful(Done))
      dbQueue.queue(record1)
      Thread.sleep(100)
    }
  }

}
