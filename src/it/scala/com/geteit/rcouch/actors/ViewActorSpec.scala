package com.geteit.rcouch.actors

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.ActorSystem
import org.scalatest.{fixture, Matchers}
import spray.http.Uri
import com.geteit.rcouch.views.{DesignDocument, ViewResponse, View}
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.pattern._
import com.geteit.rcouch.BucketSpec
import com.geteit.rcouch.actors.ViewActor.{Deleted, Saved}
import com.geteit.rcouch.Settings.ClusterSettings
import com.geteit.rcouch.views.DesignDocument.{MapFunction, ViewDef, DocumentDef}
import com.geteit.rcouch.couchbase.Couchbase.Bucket

/**
  */
class ViewActorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with fixture.FeatureSpecLike with Matchers with BucketSpec {
  def this() = this(ActorSystem("ViewActorSpec"))

  val config = ClusterSettings()

  override protected def afterAll(): Unit = {
    super.afterAll()

    TestKit.shutdownActorSystem(system)
  }

  val ddoc = DocumentDef (Map(
    "all_docs" -> ViewDef(MapFunction("function (doc, meta) { emit(meta.id, null); }"))
  ))

  withDocuments("ddoc" -> ddoc)

  feature("DesignDocument operations") {
    scenario("Save design document") { bucket: Bucket =>
      val actor = system.actorOf(ViewActor.props(Uri(bucket.nodes.head.couchApiBase.get)))
      actor ! ViewActor.SaveDesignDoc("test_doc", ddoc)
      expectMsg(Saved)
    }

    scenario("Get existing design document") { bucket: Bucket =>
      val actor = system.actorOf(ViewActor.props(Uri(bucket.nodes.head.couchApiBase.get)))
      actor ! ViewActor.GetDesignDoc("test_doc")
      expectMsg(DesignDocument("test_doc", bucket.name, ddoc.views))
    }

    scenario("Delete existing design document") { bucket: Bucket =>
      val actor = system.actorOf(ViewActor.props(Uri(bucket.nodes.head.couchApiBase.get)))
      actor ! ViewActor.DeleteDesignDoc("test_doc")
      expectMsg(Deleted)
    }
  }

  feature("View operations") {

    scenario("Try to query non existent view") { bucket: Bucket =>
      val actor = system.actorOf(ViewActor.props(Uri(bucket.nodes.head.couchApiBase.get)))
      val doc = new DesignDocument("ddoc", bucket.name, ddoc.views)

      actor ! ViewActor.QueryCommand(View("non_existent_view", doc))

      expectMsgClass(classOf[ViewResponse.Error])

      Await.result(gracefulStop(actor, 5.seconds), 6.seconds)
    }

    scenario("Connect and send view query") { bucket: Bucket =>
      // TODO: populate db with documents
      val actor = system.actorOf(ViewActor.props(Uri(bucket.nodes.head.couchApiBase.get)))
      val view = View("all_docs", new DesignDocument("ddoc", bucket.name, ddoc.views))

      actor ! ViewActor.QueryCommand(view)

      val ViewResponse.Start(count) = expectMsgClass(30.seconds, classOf[ViewResponse.Start]) // first view query can take a long time
      for (i <- 1 to count) {
        expectMsgClass(classOf[ViewResponse.Row[_]])
      }
      expectMsgClass(classOf[ViewResponse.End])

      Await.result(gracefulStop(actor, 5.seconds), 6.seconds)
    }
  }
}
