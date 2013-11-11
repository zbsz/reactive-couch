package com.geteit.rcouch.actors

import org.scalatest._
import akka.actor.{LoggingFSM, Props, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.geteit.rcouch.actors.BucketMonitor.{Bucket, Register}
import com.geteit.rcouch.Config.ClusterConfig
import com.geteit.rcouch.memcached.Memcached
import akka.util.ByteString
import com.geteit.rcouch.memcached.Memcached.{GetResponse, StoreResponse}

/**
  */
class ClusterActorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with FeatureSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ClusterSpec"))

  override protected def beforeAll() {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    TestKit.shutdownActorSystem(system)
  }

  feature("Connect to couchbase server") {
    scenario("Start ClusterActor and send memcached commands") {
      val cluster = system.actorOf(ClusterActor.props(ClusterConfig("geteit", List("http://localhost:8091/pools"))))

      cluster ! Memcached.Set("key", ByteString("value"), 0, 3600)
      val res = expectMsgClass(classOf[StoreResponse])
      res.status should be(0)

      cluster ! Memcached.Get("key")
      val gr = expectMsgClass(classOf[GetResponse])
      gr.value should be(ByteString("value"))

      cluster ! Memcached.GetK("key")
      val gr1 = expectMsgClass(classOf[GetResponse])
      gr1.key should be(Some("key"))
      gr1.value should be(ByteString("value"))
    }

    scenario("Start ClusterActor and send view query") {

    }
  }
}
