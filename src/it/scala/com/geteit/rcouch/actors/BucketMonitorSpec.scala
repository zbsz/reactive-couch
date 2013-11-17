package com.geteit.rcouch.actors

import org.scalatest._
import akka.actor.{LoggingFSM, Props, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.geteit.rcouch.actors.BucketMonitor.Register
import com.geteit.rcouch.Settings.ClusterSettings
import scala.concurrent.duration._
import akka.pattern.gracefulStop
import scala.concurrent.Await
import com.geteit.rcouch.couchbase.Couchbase.Bucket

/**
  */
class BucketMonitorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with FeatureSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("BucketMonitorSpec"))

  val config = ClusterSettings("geteit", List("http://localhost:8091/"))


  override protected def beforeAll() {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    TestKit.shutdownActorSystem(system)
  }

  feature("Connect to couchbase server") {
    scenario("Find active node and start monitoring") {
      val admin = system.actorOf(AdminActor.props(config))
      val monitor = system.actorOf(BucketMonitor.props(admin, config))

      monitor ! Register

      expectMsgClass(classOf[Bucket])
      expectMsgClass(classOf[Bucket])

      Await.result(gracefulStop(monitor, 5.seconds), 6.seconds)
    }
  }
}
