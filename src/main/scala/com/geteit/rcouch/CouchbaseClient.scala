package com.geteit.rcouch

import com.geteit.rcouch.Settings.ClusterSettings
import akka.actor.ActorSystem
import com.geteit.rcouch.actors.ClusterActor
import com.geteit.rcouch.views.{QueryExecutor, ViewResponse, View, Query}
import play.api.libs.iteratee.Enumerator
import scala.concurrent.{Await, ExecutionContext}
import concurrent.duration._
import akka.pattern._

/**
  */
class CouchbaseClient(settings: ClusterSettings, _system: ActorSystem = CouchbaseClient.defaultSystem) {

  private[rcouch] implicit val system = _system
  private[rcouch] val cluster = system.actorOf(ClusterActor.props(settings))

  def query[A](v: View, q: Query)(implicit ec: ExecutionContext): Enumerator[ViewResponse.Row[A]] = new QueryExecutor(v, q).apply(this)

  def close() {
    system.shutdown()
    Await.result(gracefulStop(cluster, 1.seconds), 1.seconds)
  }
}



object CouchbaseClient {

  def defaultSystem = ActorSystem("reactive-couch")
}