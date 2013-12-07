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
import scala.concurrent.Future
import spray.client.pipelining._
import spray.http.HttpRequest
import scala.util.Failure
import spray.http.ChunkedResponseStart
import spray.http.HttpResponse
import spray.can.Http.HostConnectorInfo
import com.geteit.rcouch.views.View
import scala.util.Success
import com.geteit.rcouch.actors.ViewActor.QueryCommand
import com.geteit.rcouch.couchbase.rest.RestApi.RestFailed
import akka.util.Timeout

/**
  */
class ViewActor(couchApiBase: Uri, user: String = "", passwd: String = "") extends Actor with Stash with ActorLogging {

  import ViewActor._
  import context.dispatcher

  private val authority = couchApiBase.authority
  private val bucket = couchApiBase.path.toString()

  IO(Http)(context.system) ! Http.HostConnectorSetup(authority.host.address, authority.port)

  def receive: Actor.Receive = {
    case _: Command => stash()
    case HostConnectorInfo(connector, _) =>
      context.become(connected(connector))
      unstashAll()
  }

  def connected(connector: ActorRef): Actor.Receive = {

    import DesignDocument.JsonProtocol._
    import spray.httpx.SprayJsonSupport._
    import concurrent.duration._

    implicit val timeout = 15.seconds: Timeout

    val pipeline: HttpRequest => Future[HttpResponse] =
      if (user == "") sendReceive(connector) else addCredentials(BasicHttpCredentials(user, passwd)) ~> sendReceive(connector)

    {
      case c: QueryCommand =>
        context.actorOf(Props(classOf[ViewQueryActor], couchApiBase, connector)).forward(c)
      case c @ GetDesignDoc(doc) =>
        val s = sender
        pipeline(Get(s"$bucket/_design/$doc")) onComplete {
          case Success(r) if r.status.isSuccess =>
            log.debug(s"GetDesignDoc response: $r")
            s ! DesignDocument(bucket, r)
          case Success(r) =>
            log.error(s"GetDesignDoc failed, for command: $c; got response: $r")
            s ! RestFailed(Uri(s"$bucket/_design/$doc"), Success(r))
          case Failure(e) =>
            log.error(e, s"GetDesignDoc failed, for command: $c")
            s ! RestFailed(Uri(s"$bucket/_design/$doc"), Failure(e))
        }
      case c @ SaveDesignDoc(doc) =>
        val s = sender
        pipeline(Put(s"$bucket/_design/${doc.name}", doc)) onComplete {
          case Success(r) if r.status.isSuccess =>
            log.debug(s"SaveDesignDoc response: $r")
            s ! Saved
          case Success(r) =>
            log.error(s"SaveDesignDoc failed, for command: $c; got response: $r")
            s ! RestFailed(Uri(s"$bucket/_design/${doc.name}"), Success(r))
          case Failure(e) =>
            log.error(e, s"SaveDesignDoc failed, for command: $c")
            s ! RestFailed(Uri(s"$bucket/_design/${doc.name}"), Failure(e))
        }
      case c @ DeleteDesignDoc(doc) =>
        val s = sender
        pipeline(Delete(s"$bucket/_design/$doc")) onComplete {
          case Success(r) if r.status.isSuccess =>
            log.debug(s"DeleteDesignDoc response: $r")
            s ! Deleted
          case Success(r) =>
            log.error(s"DeleteDesignDoc failed, for command: $c; got response: $r")
            s ! RestFailed(Uri(s"$bucket/_design/$doc"), Success(r))
          case Failure(e) =>
            log.error(e, s"DeleteDesignDoc failed, for command: $c")
            s ! RestFailed(Uri(s"$bucket/_design/$doc"), Failure(e))
        }
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
  case class SaveDesignDoc(doc: DesignDocument) extends DesignCommand

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
