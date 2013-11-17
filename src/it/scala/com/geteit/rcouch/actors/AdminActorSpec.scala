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

/**
  */
class AdminActorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with FeatureSpecLike with Matchers with BeforeAndAfterAll {
  def this() = this(ActorSystem("AdminActorSpec"))

  override protected def beforeAll() {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    TestKit.shutdownActorSystem(system)
  }

  feature("Connect to couchbase server") {
    scenario("Get bucket") {
      val actor = system.actorOf(Props(classOf[AdminActor], ClusterSettings("geteit", List("http://localhost:8091/"))))

      actor ! GetBucket("geteit")

      expectMsgClass(classOf[Bucket])

      Await.result(gracefulStop(actor, 5.seconds), 6.seconds)
    }
  }
}
