package com.geteit.rcouch.actors

import akka.actor._
import spray.can.Http
import akka.io._
import spray.http._
import com.geteit.rcouch.views._
import akka.io.IO
import com.geteit.rcouch.views.ViewResponse.HasActorSystem
import scala.concurrent.Future
import spray.client.pipelining._
import akka.util.Timeout
import spray.http.HttpRequest
import com.geteit.rcouch.views.DesignDocument.DocumentDef
import com.geteit.rcouch.couchbase.rest.RestApi.RestFailed
import scala.util.Failure
import spray.http.ChunkedResponseStart
import spray.http.HttpResponse
import spray.can.Http.HostConnectorInfo
import com.geteit.rcouch.views.View
import scala.util.Success
import com.geteit.rcouch.actors.ViewActor.QueryCommand
import java.util.concurrent.TimeUnit

/**
  */
class ViewActor(couchApiBase: Uri, user: String = "", passwd: String = "") extends Actor with Stash with ActorLogging {

  import ViewActor._
  import context.dispatcher

  private val authority = couchApiBase.authority
  private val bucket = couchApiBase.path.toString().substring(1) // TODO: pass bucket as parameter

  IO(Http)(context.system) ! Http.HostConnectorSetup(authority.host.address, authority.port)

  def receive: Actor.Receive = {
    case _: Command => stash()
    case HostConnectorInfo(connector, _) =>
      context.become(connected(connector))
      unstashAll()
  }

  def connected(connector: ActorRef): Actor.Receive = {

    import DesignDocument._
    import spray.httpx.PlayJsonSupport._

    implicit val timeout = Timeout(15, TimeUnit.SECONDS)

    val pipeline: HttpRequest => Future[HttpResponse] =
      if (user == "") sendReceive(connector) else addCredentials(BasicHttpCredentials(user, passwd)) ~> sendReceive(connector)

    def rest(c: Command, req: HttpRequest, onSuccess: HttpResponse => Any): Unit = {
      val s = sender
      pipeline(req) onComplete {
        case Success(r) if r.status.isSuccess =>
          log.debug(s"Rest completed, command: $c, response: $r")
          s ! onSuccess(r)
        case Success(r) =>
          log.error(s"Rest failed, for command: $c; got response: $r")
          s ! RestFailed(req.uri, Success(r))
        case Failure(e) =>
          log.error(e, s"Res failed, for command: $c")
          s ! RestFailed(req.uri, Failure(e))
      }
    }

    {
      case c: QueryCommand =>
        context.actorOf(Props(classOf[ViewQueryActor], couchApiBase, connector)).forward(c)
      case c @ GetDesignDoc(name) =>
        rest(c, Get(s"/$bucket/_design/$name"), DesignDocument(bucket, _))
      case c @ SaveDesignDoc(name, doc) =>
        rest(c, Put(s"/$bucket/_design/$name", doc), _ => Saved)
      case c @ DeleteDesignDoc(name) =>
        rest(c, Delete(s"/$bucket/_design/$name"), _ => Deleted)
      case m =>
        log.warning("Got unexpected message: {}", m)
    }
  }
}

object ViewActor {

  sealed trait Command
  case class QueryCommand(v: View, q: Query = Query()) extends Command

  sealed trait DesignCommand extends Command
  case class GetDesignDoc(name: String) extends DesignCommand
  case class DeleteDesignDoc(name: String) extends DesignCommand
  case class SaveDesignDoc(name: String, doc: DocumentDef) extends DesignCommand

  sealed trait Response
  case object Deleted extends Response
  case object Saved extends Response

  def props(couchApiBase: Uri, user: String = "", passwd: String = ""): Props = Props(classOf[ViewActor], couchApiBase, user, passwd)
}

class ViewQueryActor(couchApiBase: Uri, connector: ActorRef) extends Actor with ActorLogging {

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
      connector ! HttpRequest(HttpMethods.GET, couchApiBase.withPath(v.path).withQuery(q.httpQuery))
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
