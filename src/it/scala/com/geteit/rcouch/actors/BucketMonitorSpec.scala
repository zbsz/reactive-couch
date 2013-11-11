package com.geteit.rcouch.actors

import org.scalatest._
import java.net.URI
import akka.actor.{LoggingFSM, Props, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.geteit.rcouch.actors.BucketMonitor.{Bucket, Register}
import com.geteit.rcouch.Config.ClusterConfig

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
      val monitor = system.actorOf(Props(classOf[MonitorWithLogging], ClusterConfig("geteit", List("http://localhost:8091/pools"))))

      monitor ! Register

      expectMsgClass(classOf[Bucket])
      expectMsgClass(classOf[Bucket])
    }
  }
}

private class MonitorWithLogging(config: ClusterConfig)
    extends BucketMonitor(config) with LoggingFSM[BucketMonitor.State, BucketMonitor.Data]
