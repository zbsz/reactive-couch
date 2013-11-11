package com.geteit.rcouch.actors

import akka.actor._
import spray.can.Http
import akka.io._
import spray.http._
import com.geteit.rcouch.views._
import scala.collection.immutable.Queue
import akka.io.IO
import spray.http.HttpRequest
import spray.http.HttpResponse
import spray.can.Http.HostConnectorInfo
import com.geteit.rcouch.views.View
import scala.util.Success
import scala.util.Failure
import spray.http.ChunkedResponseStart
import com.geteit.rcouch.actors.ViewActor.QueryCommand
import com.geteit.rcouch.views.ViewResponse.HasActorSystem

/**
  */
class ViewActor(couchApiBase: Uri) extends Actor with ActorLogging {

  import ViewActor._
  import context._

  private val a = couchApiBase.authority
  private var queue = Queue[(ActorRef, Command)]()

  IO(Http) ! Http.HostConnectorSetup(a.host.address, a.port)

  def receive: Actor.Receive = {
    case c: QueryCommand =>
      queue = queue.enqueue((sender, c))
    case HostConnectorInfo(connector, _) =>
      context.become(connected(connector))
      queue foreach (p => self.tell(p._2, p._1))
  }

  def connected(connector: ActorRef): Actor.Receive = {
    case c: QueryCommand =>
      context.actorOf(Props(classOf[ViewQueryActor], couchApiBase)).forward(c)
    case m =>
      log.warning("Got unexpected message: {}", m)
  }

}

object ViewActor {

  sealed trait Command

  case class QueryCommand(v: View, q: Query = Query()) extends Command

  def props(couchApiBase: Uri): Props = Props(classOf[ViewActor], couchApiBase)
}

class ViewQueryActor(couchApiBase: Uri) extends Actor with ActorLogging {

  import context._

  var origSender: ActorRef = _
  lazy val pipeline = PipelineFactory.buildWithSinkFunctions(
    new HasActorSystem { val system = context.system },
    new ViewResponse.PipelineStage()
  )({_ => }, {
    case Success(resp) => origSender ! resp
    case Failure(ex) => log.error(ex, "couldn't decode view response")
  })

  def receive: Actor.Receive = {
    case QueryCommand(v, q) =>
      origSender = sender
      IO(Http) ! HttpRequest(HttpMethods.GET, couchApiBase.withPath(v.path).withQuery(q.httpQuery))
    case ChunkedResponseStart(response) =>
      log.debug("Got chunked response start:\nresponse: {}\n\nheaders: {}\n\nentity: {}", response, response.headers, response.entity)
    case MessageChunk(data, _) =>
      log.debug("Got message chunk: {}", data)
      pipeline.injectEvent(data)
    case m @ ChunkedMessageEnd(_, _) =>
      log.debug("Got chunked message end: {}", m)
      context.stop(self)
    case m @ HttpResponse(status, HttpEntity.NonEmpty(_, msg), _, _) if status.isFailure =>
      log.warning("Got error from view query: {}", m)
      origSender ! new ViewResponse.Error(msg.asString(HttpCharsets.`UTF-8`))
      context.stop(self)
    case m @ HttpResponse(status, _, _, _) if status.isFailure =>
      log.warning("Got error from view query: {}", m)
      origSender ! new ViewResponse.Error(s"Unknown error: $status")
      context.stop(self)
    case m =>
      log.warning("Got unexpected message: {}", m)
  }
}