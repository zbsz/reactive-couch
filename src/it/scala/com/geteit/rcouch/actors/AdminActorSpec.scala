package com.geteit.rcouch.actors

import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{FeatureSpecLike, BeforeAndAfterAll, Matchers}
import akka.actor.{ActorRef, Props, ActorSystem}
import concurrent.duration._
import com.geteit.rcouch.couchbase.rest.RestApi._
import com.geteit.rcouch.Settings.ClusterSettings
import com.geteit.rcouch.couchbase.Couchbase.CouchbaseException
import com.geteit.rcouch.views.DesignDocument
import scala.concurrent.{Await, Future}
import com.geteit.rcouch.actors.AdminActor.GetBucket
import com.geteit.rcouch.couchbase.rest.RestApi.GetDesignDocs
import com.geteit.rcouch.couchbase.rest.RestApi.BucketCreated
import com.geteit.rcouch.couchbase.rest.RestApi.DeleteBucket
import scala.Some
import com.geteit.rcouch.couchbase.rest.RestApi.RamQuota
import com.geteit.rcouch.couchbase.rest.RestApi.BucketDeleted
import com.geteit.rcouch.couchbase.Couchbase.Bucket
import com.geteit.rcouch.actors.ViewActor.{Saved, SaveDesignDoc}
import com.geteit.rcouch.views.DesignDocument.DocumentDef
import akka.util.Timeout
import scala.util.Random
import com.geteit.rcouch.FutureMatcher
import spray.http.Uri

/**
  */
class AdminActorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with FeatureSpecLike with Matchers with BeforeAndAfterAll with FutureMatcher {
  def this() = this(ActorSystem("AdminActorSpec"))

  val bucketName = "test_" + Random.nextInt().toHexString
  var actor: ActorRef = _
  

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    actor = system.actorOf(Props(classOf[AdminActor], ClusterSettings()))
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    Await.result(akka.pattern.ask(actor, DeleteBucket(bucketName))(10.seconds), 10.seconds)
    TestKit.shutdownActorSystem(system)
  }

  feature("Connect to couchbase server") {
    scenario("Create bucket") {
      actor ! CreateBucket(bucketName, RamQuota(100), saslPassword = Some(""), replicaNumber = ReplicaNumber.Zero)
      expectMsg(15.seconds, BucketCreated(bucketName))
    }

    scenario("Get bucket") {
      Await.result(waitForBucket(bucketName), 30.seconds)

      actor ! GetBucket(bucketName)
      expectMsgClass(classOf[Bucket])
    }

    scenario("Load design documents") {
      actor ! GetBucket(bucketName)
      val bucket = expectMsgClass(classOf[Bucket])
      val viewActor = system.actorOf(ViewActor.props(Uri(bucket.nodes.head.couchApiBase.get)))
      viewActor ! SaveDesignDoc("doc", DocumentDef(Map()))
      expectMsg(Saved)

      actor ! GetDesignDocs(bucketName)
      val msg = expectMsgClass(10.seconds, classOf[List[DesignDocument]])
      msg should have length 1
      info(s"got ${msg.length} design dosc")
    }

    scenario("Delete bucket") { bucket: Bucket =>
      actor ! DeleteBucket(bucketName)
      expectMsg(30.seconds, BucketDeleted(bucketName))
    }
  }

  def waitForBucket(bucketName: String, retry: Int = 0, delay: Long = 100): Future[Bucket] = {
    implicit val ec = system.dispatcher
    implicit val timeout = 15.seconds : Timeout
    import akka.pattern._

    if (retry > 10) Future.failed(new CouchbaseException(s"Couldn't get bucket: $bucketName"))
    else (actor ? GetBucket(bucketName)) flatMap {
      case b: Bucket if b.nodes.exists(_.healthy) => Future.successful(b)
      case _ => akka.pattern.after(delay.millis, system.scheduler)(waitForBucket(bucketName, retry + 1, delay * 2))
    }
  }
}
