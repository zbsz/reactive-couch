package com.geteit.rcouch.actors

import akka.actor._
import spray.http._
import com.geteit.rcouch.Settings.ClusterSettings
import akka.event.LoggingReceive
import com.geteit.rcouch.actors.AdminActor.{BucketNotFound, PoolRes}
import com.geteit.rcouch.couchbase.Couchbase.{Node, Bucket}
import com.geteit.rcouch.couchbase.Couchbase
import scala.util.Random

/**
  */
class BucketMonitor(adminActor: ActorRef, config: ClusterSettings) extends Actor with ActorLogging {

  import BucketMonitor._

  implicit val system = context.system

  private val user = config.user
  private val passwd = config.passwd

  private var listeners = Nil: List[ActorRef]

  private var bucket: Bucket = _
  private var nodes = Nil: List[Node]

  object JsonProtocol extends Couchbase.JsonProtocol
  import JsonProtocol._

  adminActor ! AdminActor.GetBucket(config.bucketName)

  def receive: Actor.Receive = LoggingReceive(registration orElse {
    case BucketNotFound => 
      log.error("Bucket '{}' not found", config.bucketName)
      context.stop(self)
    case b: Bucket =>
      onBucketReceived(b)
      startMonitoring()
  })

  def startMonitoring(): Unit = {
    val uri = Uri(s"http://${nodes.head.hostname}${bucket.streamingUri}")
    val monitor = system.actorOf(StreamMonitor.props[Bucket](uri, self, user, passwd))
    context.watch(monitor)
    context.become(registration orElse monitoring(uri, monitor), discardOld = true)
  }

  def onBucketReceived(b: Bucket): Unit = {
    log debug s"Found bucket: $b"
    this.bucket = b
    this.nodes = Random.shuffle(b.nodes.toList)
    listeners foreach (_ ! b)
  }

  def monitoring(uri: Uri, monitor: ActorRef): Actor.Receive = {
    case b: Bucket => onBucketReceived(b)
    case Terminated(m) =>
      log.error(s"Monitor actor has been terminated for: $uri")
      nodes = nodes.filter(n => !uri.authority.toString().contains(n.hostname))
      nodes match {
        case Nil => context.stop(self)
        case _ => startMonitoring()
      }
  }

  def registration: Actor.Receive = {
    case Register =>
      listeners ::= sender
    case Unregister =>
      listeners = listeners.filter(_ != sender)
  }
}

object BucketMonitor {
  val ClientSpecVer = "1.0"

  def props(adminActor: ActorRef, config: ClusterSettings) = Props(classOf[BucketMonitor], adminActor, config)

  sealed trait Command
  case object Register extends Command
  case object Unregister extends Command
}
