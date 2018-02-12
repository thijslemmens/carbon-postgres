package eu.thijslemmens.carbonpostgres.carbon

import java.io.DataOutputStream
import java.net.Socket

import eu.thijslemmens.carbonpostgres.{BaseTest, Record}
import eu.thijslemmens.carbonpostgres.db.DbQueue
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CarbonConnectorSpec extends BaseTest {

  "A carbon connector" must {
    "write to a dbQueue when getting messages from tcp" in {
      val dbQueue = mock[DbQueue]
      val carbonConnector = new CarbonConnector(dbQueue, "127.0.0.1", 2003)
      carbonConnector.start()

      Thread.sleep(1000)

      val carbonClient = new Socket("127.0.0.1", 2003)
      val os = new DataOutputStream(carbonClient.getOutputStream)
      val metric = "integration.test.metric1"
      val value = 12
      val timestamp = 1514056649
      os.writeBytes(s"$metric $value $timestamp")
      os.close()
      carbonClient.close()

      (dbQueue.queue(_)).expects(Record(timestamp, metric, value))

      Thread.sleep(1000)
    }
  }

}
