package com.geteit.rcouch.actors

import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{Matchers, FeatureSpecLike, BeforeAndAfterAll}
import akka.actor.{Props, ActorSystem}
import com.geteit.rcouch.Settings.ClusterSettings
import scala.concurrent.Await
import com.geteit.rcouch.actors.AdminActor.GetBucket
import concurrent.duration._
import akka.pattern._
import com.geteit.rcouch.couchbase.Couchbase.Bucket
import com.geteit.rcouch.couchbase.rest.RestApi._
import com.geteit.rcouch.Settings.ClusterSettings
import com.geteit.rcouch.actors.AdminActor.GetBucket
import com.geteit.rcouch.couchbase.Couchbase.Bucket
import scala.Some
import com.geteit.rcouch.couchbase.rest.RestApi.RamQuota
import com.geteit.rcouch.views.DesignDocument

/**
  */
class AdminActorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with FeatureSpecLike with Matchers with BeforeAndAfterAll {
  def this() = this(ActorSystem("AdminActorSpec"))

  override protected def afterAll(): Unit = {
    super.afterAll()

    TestKit.shutdownActorSystem(system)
  }

  feature("Connect to couchbase server") {
    scenario("Get bucket") {
      val actor = system.actorOf(Props(classOf[AdminActor], ClusterSettings()))

      actor ! GetBucket("geteit")

      expectMsgClass(classOf[Bucket])

      Await.result(gracefulStop(actor, 5.seconds), 6.seconds)
    }

    scenario("Create bucket") {
      val actor = system.actorOf(Props(classOf[AdminActor], ClusterSettings()))

      actor ! DeleteBucket("test") // delete if already exists
      val msg = expectMsgClass(30.seconds, classOf[RestResponse])
      info(s"trying to delete bucket, got: $msg")

      actor ! CreateBucket("test", RamQuota(100), saslPassword = Some(""), replicaNumber = ReplicaNumber.Zero)
      expectMsg(15.seconds, BucketCreated("test"))

      Await.result(gracefulStop(actor, 5.seconds), 6.seconds)
    }

    scenario("Delete bucket") {
      val actor = system.actorOf(Props(classOf[AdminActor], ClusterSettings()))

      // create if not exists
      actor ! CreateBucket("test", RamQuota(100), saslPassword = Some(""), replicaNumber = ReplicaNumber.Zero)
      val msg = expectMsgClass(30.seconds, classOf[RestResponse])
      info(s"trying to create bucket, got: $msg")

      Thread.sleep(5.seconds.toMillis) // bucket has just been created, let's wait some time

      actor ! DeleteBucket("test")
      expectMsg(30.seconds, BucketDeleted("test"))

      Await.result(gracefulStop(actor, 5.seconds), 6.seconds)
    }

    scenario("Load design documents") {
      val actor = system.actorOf(Props(classOf[AdminActor], ClusterSettings()))
      actor ! GetDesignDocs("geteit")
      val msg = expectMsgClass(10.seconds, classOf[List[DesignDocument]])
      info(s"got ${msg.length} design dosc")
    }
  }
}
