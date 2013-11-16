package com.geteit.rcouch.views

import play.api.libs.iteratee.Enumerator
import akka.actor.ActorDSL._
import com.geteit.rcouch.actors.ViewActor.QueryCommand
import com.geteit.rcouch.util.AbstractEnumerator
import com.geteit.rcouch.CouchbaseClient
import scala.concurrent.{Future, Promise, ExecutionContext}
import scala.collection.mutable
import com.geteit.rcouch.views.QueryExecutor.{BufferEnumerator, Buffer}

/**
 * Simple query executor.
 *
 */
class QueryExecutor(v: View, q: Query) {

  def apply[A](c: CouchbaseClient)(implicit ec: ExecutionContext): Enumerator[ViewResponse.Row[A]] =
    new BufferEnumerator[ViewResponse.Row[A]](prepareQueue(c))

  protected def prepareQueue[A](c: CouchbaseClient)(implicit ec: ExecutionContext): Buffer[ViewResponse.Row[A]] = {

    // TODO: implement back pressure - suspend reading results from server when buffer gets to big

    implicit val system = c.system

    val queue = new Buffer[ViewResponse.Row[A]]()

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

object QueryExecutor {

  /**
   * Closable buffer queue supporting async polling.
   * Pool method returns Future[Option[A]] that will evaluate to Some[A] immediately if queue is not empty, or once an element is added in future.
   * If queue is empty and closed returned Future will evaluate to None.
   * Once queue is closed, no more elements can be added..
   *
   * TODO: re-implement without synchronization
   *
   * @tparam A
   */
  class Buffer[A] {
    private var closed = false
    private var exception = None: Option[Exception]
    private var queue = mutable.Queue[A]()
    private var ps = mutable.Queue[Promise[Option[A]]]()

    def add(elem: A): Unit = {
      synchronized {
        if (ps.isEmpty) {
          if (!closed) queue += elem
          None
        } else Some(ps.dequeue())
      } foreach (_.success(Some(elem)))
    }

    def poll(): Future[Option[A]] = synchronized {
      if (queue.isEmpty) {
        if (closed) exception.fold(Future.successful(None))(Future.failed(_))
        else {
          val p = Promise[Option[A]]()
          ps += p
          p.future
        }
      } else {
        Future.successful(Some(queue.dequeue()))
      }
    }

    def close(): Unit = synchronized {
      closed = true
      ps.foreach(_.success(None))
      ps.clear()
    }

    def terminate(e: Exception): Unit = synchronized {
      exception = Some(e)
      closed = true
      ps.foreach(_.failure(e))
      ps.clear()
    }

    def enumerator(implicit ec: ExecutionContext): Enumerator[A] = new BufferEnumerator[A](this)
  }

  class BufferEnumerator[E](q: => Buffer[E])(implicit ec: ExecutionContext) extends AbstractEnumerator[E] {
    private lazy val queue = q

    override def nextItem(): Future[Option[E]] = queue.poll()
  }
}
