package com.geteit.rcouch.actors

import org.scalatest._
import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.geteit.rcouch.Settings.ClusterSettings
import com.geteit.rcouch.memcached.Memcached
import akka.util.{Timeout, ByteString}
import com.geteit.rcouch.memcached.Memcached.{GetResponse, StoreResponse}
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.pattern._
import com.geteit.rcouch.actors.ClusterActor.GetBucketActor
import com.geteit.rcouch.BucketSpec
import com.geteit.rcouch.couchbase.Couchbase.Bucket

/**
  */
class ClusterActorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with fixture.FeatureSpecLike with Matchers with BucketSpec {

  def this() = this(ActorSystem("ClusterSpec"))
  implicit val timeout = 5.seconds : Timeout

  override protected def beforeAll() {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    TestKit.shutdownActorSystem(system)
  }

  feature("Connect to couchbase server") {
    scenario("Get BucketActor and send memcached commands") { b: Bucket =>
      val cluster = system.actorOf(ClusterActor.props(ClusterSettings()))
      val bucket = Await.result((cluster ? GetBucketActor(b.name)).mapTo[ActorRef], 5.seconds)

      bucket ! Memcached.Set("key", ByteString("value"), 0, 3600)
      val res = expectMsgClass(classOf[StoreResponse])
      res.status should be(0)

      bucket ! Memcached.Get("key")
      val gr = expectMsgClass(classOf[GetResponse])
      gr.value should be(ByteString("value"))

      bucket ! Memcached.GetK("key")
      val gr1 = expectMsgClass(classOf[GetResponse])
      gr1.key should be(Some("key"))
      gr1.value should be(ByteString("value"))

      Await.result(gracefulStop(cluster, 5.seconds), 6.seconds)
    }
  }
}
