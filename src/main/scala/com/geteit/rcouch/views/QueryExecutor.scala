package com.geteit.rcouch.views

import play.api.libs.iteratee.Enumerator
import akka.actor.ActorDSL._
import com.geteit.rcouch.actors.ViewActor.QueryCommand
import com.geteit.rcouch.util.{CompletableQueueEnumerator, CompletableQueue}
import com.geteit.rcouch.CouchbaseClient
import scala.concurrent.ExecutionContext

/**
  */
class QueryExecutor(v: View, q: Query) {

  def apply[A](c: CouchbaseClient)(implicit ec: ExecutionContext): Enumerator[ViewResponse.Row[A]] =
    new CompletableQueueEnumerator[ViewResponse.Row[A]](prepareQueue(c))

  protected def prepareQueue[A](c: CouchbaseClient)(implicit ec: ExecutionContext): CompletableQueue[ViewResponse.Row[A]] = {

      implicit val system = c.system

      val queue = new CompletableQueue[ViewResponse.Row[A]]()

      val a = actor(new Act {
        become {
          case m: ViewResponse.End =>
            queue.close()
            context.stop(self)
          case m: ViewResponse.Error =>
            queue.terminate(m)
            context.stop(self)
          case m: ViewResponse.Row[A] =>
            queue.add(m)
        }
      })
      c.cluster.tell(QueryCommand(v, q), a)

      queue
  }
}

