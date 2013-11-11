package com.geteit.rcouch.actors

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.ActorSystem
import org.scalatest.{Matchers, FeatureSpecLike, BeforeAndAfterAll}
import spray.http.Uri
import concurrent.duration._
import com.geteit.rcouch.views.{ViewResponse, Document, View}

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

      actor ! ViewActor.QueryCommand(View("user_by_email", "geteit", "users"))

      val ViewResponse.Start(count) = expectMsgClass(classOf[ViewResponse.Start])
      for (i <- 1 to count) {
        expectMsgClass(classOf[ViewResponse.Row[_]])
      }
      expectMsgClass(classOf[ViewResponse.End])
    }

    scenario("Try to query non existent view") {
      val actor = system.actorOf(ViewActor.props(Uri("http://127.0.0.1:8092/geteit")))

      actor ! ViewActor.QueryCommand(View("non_existent_view", "geteit", "users"))

      expectMsgClass(classOf[ViewResponse.Error])
    }
  }
}