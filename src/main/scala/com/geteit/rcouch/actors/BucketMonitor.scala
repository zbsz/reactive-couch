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
class BucketMonitor(_bucket: Bucket, config: ClusterSettings) extends Actor with ActorLogging {

  implicit val system = context.system

  private val user = config.user
  private val passwd = config.passwd

  private var bucket = _bucket
  private var nodes = Random.shuffle(bucket.nodes.toList)

  object JsonProtocol extends Couchbase.JsonProtocol
  import JsonProtocol._

  override def preStart(): Unit = startMonitoring()

  def receive: Actor.Receive = PartialFunction.empty

  def startMonitoring(): Unit = {
    val uri = Uri(s"http://${nodes.head.hostname}${bucket.streamingUri}")
    val monitor = context.actorOf(StreamMonitor.props[Bucket](uri, self, user, passwd))
    context.watch(monitor)
    context.become(monitoring(uri, monitor), discardOld = true)
  }

  def monitoring(uri: Uri, monitor: ActorRef): Actor.Receive = LoggingReceive {
    case b: Bucket => onBucketReceived(b)
    case Terminated(_) =>
      log.error(s"Monitor actor has been terminated for: $uri")
      nodes = nodes.filter(n => !uri.authority.toString().contains(n.hostname))
      nodes match {
        case Nil => context.stop(self)
        case _ => startMonitoring()
      }
  }

  def onBucketReceived(b: Bucket): Unit = {
    log debug s"Found bucket: $b"
    this.bucket = b
    this.nodes = Random.shuffle(b.nodes.toList)
    context.parent ! b
  }
}

object BucketMonitor {
  def props(bucket: Bucket, config: ClusterSettings) = Props(classOf[BucketMonitor], bucket, config)
}
