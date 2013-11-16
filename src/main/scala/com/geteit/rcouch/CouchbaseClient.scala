package com.geteit.rcouch

import com.geteit.rcouch.Settings.ClusterSettings
import akka.actor.{ActorRef, ActorSystem}
import com.geteit.rcouch.actors.ClusterActor
import com.geteit.rcouch.views.{QueryExecutor, ViewResponse, View, Query}
import play.api.libs.iteratee.Enumerator
import scala.concurrent.{Await, ExecutionContext}
import concurrent.duration._
import akka.pattern._
import com.geteit.rcouch.memcached.MemcachedClient

trait Client {
  implicit val system: ActorSystem
  implicit val ec: ExecutionContext
  val cluster: ActorRef
}

/**
  */
class CouchbaseClient(settings: ClusterSettings, _system: ActorSystem = CouchbaseClient.defaultSystem)
                     (implicit val ec: ExecutionContext = _system.dispatcher)
    extends Client with MemcachedClient {

  override implicit val system = _system
  override val cluster = system.actorOf(ClusterActor.props(settings))

  def query[A](v: View, q: Query): Enumerator[ViewResponse.Row[A]] = new QueryExecutor(v, q).apply(this)

  def close() {
    system.shutdown()
    Await.result(gracefulStop(cluster, 1.seconds), 1.seconds)
  }
}

object CouchbaseClient {

  def defaultSystem = ActorSystem("reactive-couch")
}

