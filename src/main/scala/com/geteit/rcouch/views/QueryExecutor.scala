package com.geteit.rcouch.views

import play.api.libs.iteratee.Enumerator
import com.geteit.rcouch.actors.ViewActor.QueryCommand
import com.geteit.rcouch.{BucketClient, CouchbaseClient}
import scala.concurrent.{Future, ExecutionContext}
import scala.collection.mutable
import akka.actor.{Props, ActorRef, Actor}
import com.geteit.rcouch.views.InboxActor.Get
import akka.util.Timeout
import concurrent.duration._

/**
 * Simple query executor.
 *
 */
class QueryExecutor(v: View, q: Query)(implicit timeout: Timeout = 15.seconds) {

  def apply(c: ViewClient)(implicit ec: ExecutionContext): Enumerator[ViewResponse.Row] = {
    // TODO: implement back pressure - suspend reading results from server when buffer gets to big

    implicit val system = c.system
    import akka.pattern.ask

    val inbox = system.actorOf(Props(classOf[InboxActor]))
    c.actor.tell(QueryCommand(v, q), inbox)

    def nextItem: Future[Option[ViewResponse.Row]] = ask(inbox, Get) flatMap {
      case m: ViewResponse.End =>
        system.stop(inbox)
        Future.successful(None)
      case m: ViewResponse.Error =>
        system.stop(inbox)
        Future.failed(m)
      case m: ViewResponse.Row =>
        Future.successful(Some(m))
      case _ => nextItem
    }

    Enumerator.generateM(nextItem)
  }
}

class InboxActor extends Actor {
  // TODO: buffer should probably be bounded
  private val buffer = mutable.Queue[Any]()
  private val clients = mutable.Queue[ActorRef]()

  def receive: Actor.Receive = {
    case Get =>
      if (buffer.isEmpty) clients += sender
      else sender ! buffer.dequeue()
    case m =>
      if (clients.isEmpty) buffer += m
      else clients.dequeue() ! m
  }
}

object InboxActor {
  case object Get
}
