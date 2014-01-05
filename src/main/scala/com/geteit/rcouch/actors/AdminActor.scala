package com.geteit.rcouch.actors

import akka.actor._
import spray.http._
import scala.concurrent.Future
import com.geteit.rcouch.couchbase.ChunkedParserPipelineStage
import akka.io.{HasActorContext, PipelineFactory, IO}
import spray.can.Http
import concurrent.duration._
import akka.event.{LoggingReceive, Logging}
import com.geteit.rcouch.couchbase.rest.RestApi._
import spray.client.pipelining._
import com.geteit.rcouch.Settings.ClusterSettings
import com.geteit.rcouch.couchbase.rest.RestApi.RestFailed
import spray.http.HttpHeaders.RawHeader
import scala.util.Failure
import spray.http.ChunkedResponseStart
import com.geteit.rcouch.couchbase.rest.RestApi.DeleteBucket
import spray.http.HttpResponse
import scala.util.Success
import spray.http.HttpRequest
import com.geteit.rcouch.couchbase.rest.RestApi.BucketCreated
import akka.actor.Terminated
import com.geteit.rcouch.couchbase.Couchbase._
import com.geteit.rcouch.views.DesignDocument
import play.api.libs.json.{Reads, Json}

/**
  */
class AdminActor(config: ClusterSettings) extends Actor with ActorLogging with Stash {

  import AdminActor._

  private val user = config.user
  private val passwd = config.passwd

  private var bucketsUri: Uri = _
  private var buckets = Array[Bucket]()
  private var nodes = configNodes
  private var retries = 0
  private var retryDelay = 1000

  implicit val system = context.system
  import system.dispatcher

  val pipelines = new Pipelines(user, passwd)
  import pipelines._

  tryNode(nodes.head)

  def receive: Actor.Receive = LoggingReceive {
    case NodeCheckFailed(uri) =>
      nodes = nodes.filter(n => !uri.authority.toString().contains(n.hostname))
      nodes match {
        case Nil =>
          if (retries >= config.connection.maxReconnectAttempts) context.stop(self)
          else {
            retries += 1
            retryDelay *= 2
            nodes = configNodes
            system.scheduler.scheduleOnce(retryDelay.milliseconds)(tryNode(nodes.head))
          }
        case n :: ns => tryNode(n)
      }
    case StartMonitoring(streamingUri) =>
      retries = 0
      retryDelay = 1000
      val monitor = system.actorOf(StreamMonitor.props[PoolRes](streamingUri, self, user, passwd)(PoolResFormat))
      context.watch(monitor)
      context.become(running(streamingUri, monitor) orElse restHandling)
      unstashAll()
    case _ => stash()
  }

  def running(streamingUri: Uri, monitor: ActorRef): Actor.Receive = LoggingReceive {
    case PoolRes(bUri, ns) =>
      bucketsUri = resolveUri(streamingUri, bUri.uri)
      nodes = ns.toList
      loadBuckets(bucketsUri)
    case bs: Array[Bucket] =>
      this.buckets = bs
    case NodeCheckFailed(uri) =>
      context.unwatch(monitor)
      context.stop(monitor)
      context.unbecome()
      self ! NodeCheckFailed(uri)
    case Terminated(m) =>
      log.error(s"Monitor actor has been terminated for: $streamingUri")
      context.unbecome()
      self ! NodeCheckFailed(streamingUri)
  }

  def restHandling: Receive = {
    case GetBucket(name) =>
      sender ! buckets.find(_.name == name).getOrElse(BucketNotFound)
    case c: CreateBucket =>
      rest(c, Post(bucketsUri, c), _ => BucketCreated(c.name))
    case c @ DeleteBucket(name) =>
      rest(c, Delete(bucketsUri.withPath(bucketsUri.path / name)), _ => BucketDeleted(name))
    case c @ GetDesignDocs(bucket) =>
      rest(c, Get(bucketsUri.withPath(bucketsUri.path / bucket / "ddocs")), DesignDocument.ddocs(bucket, _))
  }

  private def rest(c: RestCommand, req: HttpRequest, onSuccess: HttpResponse => Any): Unit = {
    val s = sender
    pipeline(req) onComplete {
      case Success(r) if r.status.isSuccess =>
        log.debug(s"Rest success, command: $c, response: $r")
        s ! onSuccess(r)
      case Success(r) =>
        log.error(s"Rest failed, for command: $c; got response: $r")
        s ! RestFailed(req.uri, Success(r))
      case Failure(e) =>
        log.error(e, s"Rest failed, for command: $c")
        s ! RestFailed(req.uri, Failure(e))
    }
  }


  private def loadBuckets(uri: Uri) = bucketsPipeline(Get(uri)) onComplete {
    case Success(bs) => self ! bs
    case Failure(e) =>
      log.error(e, s"Exception while loading buckets list for: $uri")
      self ! NodeCheckFailed(uri)
  }

  private def tryNode(node: Node): Unit = {
    val uri = Uri("http://" + node.hostname)
    (for {
      DefaultPool(pool) <- poolsPipeline(Get(resolveUri(uri, "/pools")))
      poolRes <- poolPipeline(Get(resolveUri(uri, pool.uri)))
      bucketsUri = resolveUri(uri, poolRes.buckets.uri)
      buckets <- bucketsPipeline(Get(bucketsUri))
    } yield (resolveUri(uri, pool.streamingUri), poolRes.nodes, bucketsUri, buckets)) onComplete {
      case Success((streamingUri, ns, bUri, bs)) =>
        this.nodes = ns.toList
        this.bucketsUri = bUri
        this.buckets = bs
        self ! StartMonitoring(streamingUri)
      case Failure(e) =>
        log.error(e, s"Exception while loading buckets for: $uri")
        self ! NodeCheckFailed(uri)
    }
  }

  private def configNodes = config.hosts.map(Uri(_)).map { uri =>
    Node(None, s"${uri.authority.host}:${uri.authority.port}", "", Ports(0, 0))
  }
}

object AdminActor {

  sealed trait Command
  case class StartMonitoring(streamingUri: Uri) extends Command
  case class NodeCheckFailed(uri: Uri) extends Command
  case class GetBucket(name: String) extends Command

  sealed trait Response
  case object BucketNotFound extends Response

  case class PoolUri(name: String, uri: String, streamingUri: String)
  case class BucketsUri(uri: String)
  case class PoolRes(buckets: BucketsUri, nodes: List[Node])
  case class PoolsRes(pools: List[PoolUri])

  def resolveUri(base: Uri, uri: String) = Uri(uri).resolvedAgainst(base)

  def props(config: ClusterSettings) = Props(classOf[AdminActor], config)

  object DefaultPool {
    def unapply(res: PoolsRes) = res.pools.find(_.name == "default").orElse(res.pools.headOption)
  }

  implicit val BucketUriFormat = Json.format[BucketsUri]
  implicit val PoolUriFormat   = Json.format[PoolUri]
  implicit val PoolResFormat   = Json.format[PoolRes]
  implicit val PoolsResFormat  = Json.format[PoolsRes]

  class Pipelines(user: String, passwd: String)(implicit val system: ActorSystem) {
    import system.dispatcher

    val log = Logging(system, "AdminActor.Pipelines")
    import spray.httpx.PlayJsonSupport._

    val pipeline: HttpRequest => Future[HttpResponse] =
      if (user == "") sendReceive else addCredentials(BasicHttpCredentials(user, passwd)) ~> sendReceive

    val poolsPipeline: HttpRequest => Future[PoolsRes] = (
      (if (user == "") sendReceive else addCredentials(BasicHttpCredentials(user, passwd)) ~> sendReceive)
        ~> unmarshal[PoolsRes]
      )

    val poolPipeline: HttpRequest => Future[PoolRes] = (
      (if (user == "") sendReceive else addCredentials(BasicHttpCredentials(user, passwd)) ~> sendReceive)
        ~> unmarshal[PoolRes]
      )

    val bucketsPipeline: HttpRequest => Future[Array[Bucket]] = (
      (if (user == "") sendReceive else addCredentials(BasicHttpCredentials(user, passwd)) ~> sendReceive)
        ~> unmarshal[Array[Bucket]]
      )
  }
}

object StreamMonitor {
  val ClientSpecVer = "1.0"

  def props[A <: AnyRef : Reads](streamingUri: Uri, parent: ActorRef, user: String, passwd: String) =
    Props(classOf[StreamMonitor[A]], streamingUri, parent, user, passwd, implicitly[Reads[A]])
}

class StreamMonitor[A <: AnyRef](streamingUri: Uri, parent: ActorRef, user: String, passwd: String, reader: Reads[A]) extends Actor with ActorLogging {

  implicit val system = context.system

  val pipeline = PipelineFactory.buildWithSinkFunctions(
    new HasActorContext { def getContext: ActorContext = context },
    new ChunkedParserPipelineStage[A]()(reader)
  )({_ => }, {
    case Success(res) => parent ! res
    case Failure(e) => log.error(e, "couldn't decode pools response")
  })

  IO(Http) ! HttpRequest(HttpMethods.GET, streamingUri, headers)

  def receive = {
    case ChunkedResponseStart(res) => log.debug("start: " + res)
    case ChunkedMessageEnd(ext, trailer) => log.debug("end: " + ext)
      context.stop(self)
    case m: HttpMessagePart => pipeline.injectEvent(m)
    case msg => log.warning(s"Received unknown message: $msg")
  }

  private def headers = {
    val headers = List(
      HttpHeaders.`User-Agent`("reactive-couch vbucket client"),
      RawHeader("X-memcachekv-Store-Client-Specification-Version", StreamMonitor.ClientSpecVer)
    )
    if (user == "") headers
    else HttpHeaders.Authorization(BasicHttpCredentials(user, passwd)) :: headers
  }
}
