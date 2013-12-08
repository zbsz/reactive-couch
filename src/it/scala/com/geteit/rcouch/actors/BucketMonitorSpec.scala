package com.geteit.rcouch.actors

import org.scalatest._
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.geteit.rcouch.Settings.ClusterSettings
import scala.concurrent.duration._
import akka.pattern.gracefulStop
import scala.concurrent.Await
import com.geteit.rcouch.couchbase.Couchbase.Bucket
import akka.actor.ActorDSL._
import com.geteit.rcouch.actors.AdminActor.GetBucket

/**
  */
class BucketMonitorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with FeatureSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("BucketMonitorSpec"))

  val config = ClusterSettings()


  override protected def beforeAll() {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    TestKit.shutdownActorSystem(system)
  }

  feature("Connect to couchbase server") {
    scenario("Find active node and start monitoring") {

      val a = actor(system, "parent")(new Act {
        context.actorOf(AdminActor.props(config)) ! GetBucket("geteit")

        become {
          case bucket: Bucket =>
            context.actorOf(BucketMonitor.props(bucket, config))
            become {
              case x ⇒ testActor ! x
            }
          case x ⇒ testActor ! x
        }
      })

      expectMsgClass(10.seconds, classOf[Bucket])

      Await.result(gracefulStop(a, 5.seconds), 6.seconds)
    }
  }
}
