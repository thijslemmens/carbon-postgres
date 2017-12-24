package eu.thijslemmens.carbonpostgres.integrationtest

import java.io.DataOutputStream
import java.net.Socket
import java.sql.{DriverManager, ResultSet}
import java.util.TimeZone

import org.junit.runner.RunWith
import org.scalamock.matchers.Matchers
import org.scalamock.scalatest.MockFactory
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.junit.Assert._


@RunWith(classOf[JUnitRunner])
class TcpTest extends WordSpecLike with Matchers with BeforeAndAfterAll with MockFactory{

  val timeZone: TimeZone = TimeZone.getTimeZone("GMT");
  TimeZone.setDefault(timeZone);

  val tcpHost = System.getProperty("tcp.host")
  val tcpPort = System.getProperty("tcp.port").toInt
  val tsHost = System.getProperty("timescaledb.host")
  val tsPort = System.getProperty("timescaledb.port").toInt

  "writing a metric using tcp" must {
    "make the metric available in postgres" in {
      val carbonClient = new Socket(tcpHost, tcpPort)
      val os = new DataOutputStream(carbonClient.getOutputStream)
      val metric = "integration.test.metric1"
      val value = 12
      val timestamp = 1514056649
      os.writeBytes(s"$metric $value $timestamp")
      os.close()
      carbonClient.close()

      Thread.sleep(500)

      val connection = DriverManager.getConnection(s"jdbc:postgresql://$tsHost:$tsPort/timescale", "timescale", "timescale")

      val query = s"select * from records where time = to_timestamp($timestamp) and value = 12 and metric='$metric'"
      val result: ResultSet = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(query)
      assertTrue(result.last())
      assertEquals(1, result.getRow())
    }
  }

}
