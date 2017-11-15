package eu.thijslemmens.carbonpostgres

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec
import org.scalatest._

import scala.concurrent.Future
import scala.concurrent.duration._



class DbQueueSpec extends TestKit(ActorSystem("testActorSystem")) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll with MockFactory{

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "A DbQueue" must{
    "write to a db when offering a record" in {
      val dbWriter = mock[DbWriter]
      val dbQueueFactory: DbQueueFactory = new DbQueueFactory(dbWriter, 1)
      val dbQueue: DbQueue = dbQueueFactory.getDbQueue()

      val record1 = new Record(1234567, "test.metric", 1234)
      val record2 = new Record(1235434567, "test.metric", 1234)
      val record3 = new Record(34567, "test.metric", 124347)
      val record4 = new Record(34567, "test.metric", 124345)
      val record5 = new Record(34567, "test.metric", 124346)



      within(200.milliseconds){
        (dbWriter.write(_: List[Record])).expects(List(record1))
//        (dbWriter.write(_: List[Record])).when(List(record1)).onCall((_: List[Record]) => {
//          println("hello world")
//          Thread.sleep(1000)
//          Future.successful(Done)
//        })
        (dbWriter.write(_: List[Record])).expects(List(record2,record3))

        dbQueue.queue(record1)
        dbQueue.queue(record2)
        dbQueue.queue(record3)


      }

    }
  }

}
