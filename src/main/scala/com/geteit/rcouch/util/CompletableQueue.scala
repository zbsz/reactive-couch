package com.geteit.rcouch.util

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import play.api.libs.iteratee.Enumerator

class CompletableQueue[A] {
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

  def enumerator(implicit ec: ExecutionContext): Enumerator[A] = new CompletableQueueEnumerator[A](this)
}
