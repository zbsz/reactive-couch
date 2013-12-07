package com.geteit.rcouch.actors

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.ActorSystem
import org.scalatest.{Matchers, FeatureSpecLike, BeforeAndAfterAll}
import spray.http.Uri
import com.geteit.rcouch.views.{DesignDocument, ViewResponse, Document, View}
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.pattern._
import com.geteit.rcouch.FutureMatcher
import com.geteit.rcouch.actors.ViewActor.{Deleted, Saved}
import com.geteit.rcouch.couchbase.rest.RestApi.RestFailed

/**
  */
class ViewActorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with FeatureSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ViewActorSpec"))

  override protected def beforeAll() {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    TestKit.shutdownActorSystem(system)
  }

  feature("ViewActor connection to couchbase server") {
    scenario("Connect and send view query") {
      val actor = system.actorOf(ViewActor.props(Uri("http://127.0.0.1:8092/geteit")))
      val doc = new DesignDocument("users", "geteit", Map())
      actor ! ViewActor.QueryCommand(View("user_by_email", doc))

      val ViewResponse.Start(count) = expectMsgClass(classOf[ViewResponse.Start])
      for (i <- 1 to count) {
        expectMsgClass(classOf[ViewResponse.Row[_]])
      }
      expectMsgClass(classOf[ViewResponse.End])

      Await.result(gracefulStop(actor, 5.seconds), 6.seconds)
    }

    scenario("Try to query non existent view") {
      val actor = system.actorOf(ViewActor.props(Uri("http://127.0.0.1:8092/geteit")))
      val doc = new DesignDocument("users", "geteit", Map())

      actor ! ViewActor.QueryCommand(View("non_existent_view", doc))

      expectMsgClass(classOf[ViewResponse.Error])

      Await.result(gracefulStop(actor, 5.seconds), 6.seconds)
    }
  }

  feature("DesignDocument operations") {
    scenario("Save empty design document") {
      val actor = system.actorOf(ViewActor.props(Uri("http://127.0.0.1:8092/geteit")))
      val doc = new DesignDocument("test_doc", "geteit", Map())

      actor ! ViewActor.DeleteDesignDoc("test_doc")
      receiveOne(5.seconds)

      actor ! ViewActor.SaveDesignDoc(doc)
      expectMsg(Saved)
    }

    scenario("Delete existing design document") {
      val actor = system.actorOf(ViewActor.props(Uri("http://127.0.0.1:8092/geteit")))
      val doc = new DesignDocument("test_doc", "geteit", Map())

      actor ! ViewActor.SaveDesignDoc(doc)
      receiveOne(5.seconds)

      actor ! ViewActor.DeleteDesignDoc("test_doc")
      expectMsg(Deleted)
    }

    scenario("Get existing design document") {
      val actor = system.actorOf(ViewActor.props(Uri("http://127.0.0.1:8092/geteit")))
      val doc = new DesignDocument("test_doc", "geteit", Map())

      actor ! ViewActor.DeleteDesignDoc("test_doc")
      receiveOne(5.seconds)

      actor ! ViewActor.SaveDesignDoc(doc)
      expectMsg(Saved)

      actor ! ViewActor.GetDesignDoc("test_doc")
      expectMsg(doc)
    }
  }
}
