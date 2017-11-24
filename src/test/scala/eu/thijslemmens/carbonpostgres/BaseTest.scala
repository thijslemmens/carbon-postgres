package eu.thijslemmens.carbonpostgres

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import org.scalamock.matchers.Matchers
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext

abstract class BaseTest extends TestKit(ActorSystem("testActorSystem")) with WordSpecLike with Matchers with BeforeAndAfterAll with MockFactory  {
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val materializer = ActorMaterializer()
  implicit val timeout = Timeout(5.seconds)

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
}
