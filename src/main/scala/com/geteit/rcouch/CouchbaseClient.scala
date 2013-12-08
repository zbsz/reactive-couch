package com.geteit.rcouch

import com.geteit.rcouch.Settings.ClusterSettings
import akka.actor.{ActorRef, ActorSystem}
import com.geteit.rcouch.actors.ClusterActor
import com.geteit.rcouch.views._
import scala.concurrent.{Future, Await, ExecutionContext}
import concurrent.duration._
import akka.pattern._
import com.geteit.rcouch.memcached.MemcachedClient
import akka.util.Timeout
import com.geteit.rcouch.actors.BucketActor.BucketActorRef
import com.geteit.rcouch.couchbase.AdminClient
import com.geteit.rcouch.actors.ClusterActor.GetBucketActor

trait Client {
  implicit val system: ActorSystem
  implicit val ec: ExecutionContext
  implicit val timeout: Timeout
  val actor: ActorRef
}

/**
  */
class CouchbaseClient(settings: ClusterSettings = ClusterSettings(), _system: ActorSystem = CouchbaseClient.defaultSystem)
                     (implicit val ec: ExecutionContext = _system.dispatcher)
    extends Client with AdminClient {

  override implicit val system = _system
  override val actor = system.actorOf(ClusterActor.props(settings))
  implicit val timeout: Timeout = 15.seconds

  def bucket(name: String): Future[BucketClient] = ask(actor, GetBucketActor(name)) map {
    case actor: ActorRef => new BucketClient(name, actor, this)
  }

  def close() {
    system.shutdown()
    Await.result(gracefulStop(actor, 1.seconds), 1.seconds)
  }
}

object CouchbaseClient {

  def defaultSystem = ActorSystem("reactive-couch")
}

class BucketClient(val bucket: String, val actor: BucketActorRef, val parent: CouchbaseClient)
                  (implicit val system: ActorSystem, val timeout: Timeout)
    extends Client with MemcachedClient with ViewClient {

  implicit val ec: ExecutionContext = system.dispatcher
}

