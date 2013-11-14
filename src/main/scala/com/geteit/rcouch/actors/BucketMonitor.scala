package com.geteit.rcouch.actors

import akka.actor._
import java.net.URI
import com.geteit.rcouch.actors.BucketMonitor.{State, Data}
import spray.http._
import spray.client.pipelining._
import scala.concurrent.Future
import spray.json.{JsonParser, DefaultJsonProtocol}
import spray.httpx.unmarshalling._
import concurrent.duration._
import spray.http.HttpRequest
import scala.util.Failure
import scala.Some
import spray.http.HttpResponse
import com.geteit.rcouch.Settings.ClusterSettings
import scala.util.Success
import akka.event.Logging
import akka.io.IO
import spray.can.Http
import spray.http.HttpHeaders.RawHeader

/**
  */
class BucketMonitor(config: ClusterSettings)
    extends FSM[State, Data] with ActorLogging {

  import BucketMonitor._

  implicit val system = context.system
  import system.dispatcher

  private val bucketName = config.bucketName
  private val user = config.user
  private val passwd = config.passwd
  private val baseNodes = config.hosts.map(URI.create)

  private var topologyListeners = Nil: List[ActorRef]
  private val configLoader = new BootstrapConfigLoader(bucketName, user, passwd)

  startWith(Initializing, InitData(baseNodes))

  when(Initializing) {
    case Event(Initialize, InitData(_, retries, delay)) =>
      if (retries >= config.connection.maxReconnectAttempts) {
        log error s"Initialization failed after $retries retries"
        context.stop(self)
        stay()
      } else {
        val nodes = util.Random.shuffle(baseNodes)
        configLoader.checkNode(nodes.head, self)
        stay using InitData(nodes, retries + 1, delay * 2)
      }
    case Event(NodeCheckFailed, d @ InitData(node :: left, _, _)) =>
      log debug s"Failed init for node: $node"

      left match {
        case Nil =>
          context.system.scheduler.scheduleOnce(d.retryDelay.seconds, self, Initialize)
          stay()
        case nodes =>
          configLoader.checkNode(baseNodes.head, self)
          stay using d.copy(nodes = nodes)
      }
    case Event(b: Bucket, InitData(node :: _, _, _)) =>
      log debug s"Found bucket: $b"
      topologyListeners foreach (_ ! b)
      val streamingUri = Uri(node.resolve(b.streamingUri).toString)
      val streamingActor = system.actorOf(Props.create(classOf[StreamMonitor], streamingUri, self, user, passwd))
      context.watch(streamingActor)
      goto(Monitoring) using MonitoringData(b)
  }

  when(Monitoring) {
    case Event(b: Bucket, _) =>
      topologyListeners foreach (_ ! b)
      stay() using MonitoringData(b)
  }

  whenUnhandled {
    case Event(Register, _) =>
      topologyListeners ::= sender
      stay()
    case Event(Unregister, _) =>
      topologyListeners = topologyListeners.filter(_ != sender)
      stay()
  }

  initialize()

  self ! Initialize
}

object BucketMonitor {

  val ClientSpecVer = "1.0"

  def apply(config: ClusterSettings) = Props[BucketMonitor](new BucketMonitor(config))

  sealed trait Command
  case object Register extends Command
  case object Unregister extends Command
  case object Initialize extends Command
  case object NodeCheckFailed extends Command

  sealed trait State
  case object Initializing extends State
  case object Monitoring extends State

  sealed trait Data
  case class InitData(nodes: Seq[URI], retries: Int = 0, retryDelay: Int = 1) extends Data
  case class MonitoringData(bucket: Bucket) extends Data


  case class Ports(proxy: Int, direct: Int)
  case class Node(couchApiBase: String, hostname: String, status: String, ports: Ports)
  case class VBucketMap(hashAlgorithm: String, numReplicas: Int, serverList: Array[String], vBucketMap: Array[Array[Int]])
  case class Bucket(name: String, uri: String, streamingUri: String, saslPasswd: Option[String], nodes: Array[Node], vBucketServerMap: VBucketMap)
}

class StreamMonitor(streamingUri: Uri, monitor: ActorRef, user: String, passwd: String) extends Actor with ActorLogging {

  implicit val system = context.system
  val protocol = new JsonProtocol
  import protocol._

  IO(Http) ! HttpRequest(HttpMethods.GET, streamingUri, headers)

  def receive = {
      case ChunkedResponseStart(res) =>
        log.info("start: " + res)
      case MessageChunk(body, ext) =>
        log.info("chunk: " + body)
        BucketChunk.unapply(body) foreach (monitor ! _)
      case ChunkedMessageEnd(ext, trailer) =>
        log.info("end: " + ext)
        context.stop(self)
      case msg =>
        log.warning(s"Received unknown message: $msg")
  }

  private def headers = {
    val headers = List(
      HttpHeaders.`User-Agent`("reactive-couch vbucket client"),
      RawHeader("X-memcachekv-Store-Client-Specification-Version", BucketMonitor.ClientSpecVer)
    )
    if (user == "") headers
    else HttpHeaders.Authorization(BasicHttpCredentials(user, passwd)) :: headers
  }
}

/**
 * Loads bucket config from RESTful interface.
 * 
 * @param bucketName
 * @param user
 * @param passwd
 * @param system
 */
class BootstrapConfigLoader(bucketName: String, user: String, passwd: String)(implicit system: ActorSystem) {

  import BucketMonitor._
  import system.dispatcher

  val log = Logging(system, classOf[BootstrapConfigLoader].getName)
  val protocol = new JsonProtocol
  import protocol._

  val pipeline: HttpRequest => Future[HttpResponse] =
    if (user == "") sendReceive
    else addCredentials(BasicHttpCredentials(user, passwd)) ~> sendReceive


  def checkNode(uri: URI, actor: ActorRef): Unit = {
    (for {
      DefaultPoolUri(poolUri) <- pipeline(Get(uri.toString))
      BucketsUri(bucketsUri) <- pipeline(Get(uri.resolve(poolUri).toString))
      BucketsArray(buckets) <- pipeline(Get(uri.resolve(bucketsUri).toString))
    } yield buckets.find(_.name == bucketName).get) onComplete {
      case Success(bucket) =>
        actor ! bucket
      case Failure(e) =>
        log.error(e, s"Exception while loading BucketsConfig for: $uri")
        actor ! NodeCheckFailed
    }
  }
}

object BootstrapConfigLoader {
  case class PoolUri(name: String, uri: String)
  case class BucketUri(uri: String)
  case class PoolsRes(pools: Array[PoolUri])
  case class PoolRes(buckets: BucketUri)
}

class JsonProtocol(implicit system: ActorSystem) extends DefaultJsonProtocol {
  val log = Logging(system, "BucketMonitorJsonProtocol")

  import BucketMonitor._
  import BootstrapConfigLoader._
  import spray.httpx.SprayJsonSupport._

  implicit val PoolUriFormat = jsonFormat2(PoolUri)
  implicit val PoolsResFormat = jsonFormat1(PoolsRes)
  implicit val BucketUriFormat = jsonFormat1(BucketUri)
  implicit val PoolResFormat = jsonFormat1(PoolRes)
  implicit val PortsFormat = jsonFormat2(Ports)
  implicit val NodeFormat = jsonFormat4(Node)
  implicit val VBucketMapFormat = jsonFormat4(VBucketMap)
  implicit val BucketFormat = jsonFormat6(Bucket)


  object DefaultPoolUri {
    def unapply(res: HttpResponse): Option[String] = {
      if (res.status.isSuccess) {
        res.entity.as[PoolsRes].fold(_ => None, Some(_)).flatMap(_.pools.find(_.name == "default")).map(_.uri)
      } else {
        log.warning("Pools response failed: {}", res)
        None
      }
    }
  }

  object BucketsUri {
    def unapply(res: HttpResponse): Option[String] = {
      if (res.status.isSuccess) {
        res.entity.as[PoolRes].fold(_ => None, Some(_)).map(_.buckets.uri)
      } else {
        log.warning("Pool response failed: {}", res)
        None
      }
    }
  }

  object BucketsArray {
    def unapply(res: HttpResponse): Option[Array[Bucket]] = {
      if (res.status.isSuccess) {
        res.entity.as[Array[Bucket]].fold(_ => None, Some(_))
      } else {
        log.warning("Buckets response failed: {}", res)
        None
      }
    }
  }

  object BucketChunk {
    def unapply(body: HttpData.NonEmpty): Option[Bucket] = {
      try {
        val str = body.asString(HttpCharsets.`UTF-8`).trim
        if (str.isEmpty) None
        else Some(JsonParser(str).convertTo[Bucket])
      } catch {
        case e: Exception =>
          log.error(e, "Error while decoding bucket chunk")
          None
      }
    }
  }
}
