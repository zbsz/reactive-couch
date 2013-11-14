package com.geteit.rcouch.util

import play.api.libs.iteratee.{Input, Step, Iteratee, Enumerator}
import scala.concurrent.{ExecutionContext, Promise, Future}
import scala.util.{Failure, Success}


abstract class AbstractEnumerator[E](implicit ec: ExecutionContext) extends Enumerator[E] {

  def apply[A](it: Iteratee[E, A]): Future[Iteratee[E, A]] = {

    val iterateeP = Promise[Iteratee[E, A]]()

    def step(it: Iteratee[E, A]) {

      val next = it.fold {
        case Step.Cont(k) => {
          nextItem().map {
            case Some(read) => Some(k(Input.El(read)))
            case None =>
              iterateeP.success(k(Input.EOF))
              None
          }
        }
        case _ => { iterateeP.success(it); Future.successful(None) }
      }

      next.onComplete {
        case Success(Some(i)) => step(i)
        case Failure(e) =>
          iterateeP.failure(e)
        case _ =>
          onFinished()
      }

    }

    step(it)
    iterateeP.future
  }

  def nextItem(): Future[Option[E]]

  def onFinished() {}
}

class CompletableQueueEnumerator[E](q: => CompletableQueue[E])(implicit ec: ExecutionContext) extends AbstractEnumerator[E] {
  private lazy val queue = q

  override def nextItem(): Future[Option[E]] = queue.poll()
  override def onFinished(): Unit = queue.close()
}