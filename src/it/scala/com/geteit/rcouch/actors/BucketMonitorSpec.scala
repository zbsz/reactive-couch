package com.geteit.rcouch.actors

import org.scalatest._
import akka.actor.{LoggingFSM, Props, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.geteit.rcouch.actors.BucketMonitor.{Bucket, Register}
import com.geteit.rcouch.Settings.ClusterSettings
import scala.concurrent.duration._
import akka.pattern.gracefulStop
import scala.concurrent.Await

/**
  */
class BucketMonitorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with FeatureSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("BucketMonitorSpec"))



  override protected def beforeAll() {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    TestKit.shutdownActorSystem(system)
  }

  feature("Connect to couchbase server") {
    scenario("Find active node and start monitoring") {
      val monitor = system.actorOf(Props(classOf[MonitorWithLogging], ClusterSettings("geteit", List("http://localhost:8091/pools"))))

      monitor ! Register

      expectMsgClass(classOf[Bucket])
      expectMsgClass(classOf[Bucket])

      Await.result(gracefulStop(monitor, 5.seconds), 6.seconds)
    }
  }
}

private class MonitorWithLogging(config: ClusterSettings)
    extends BucketMonitor(config) with LoggingFSM[BucketMonitor.State, BucketMonitor.Data]
