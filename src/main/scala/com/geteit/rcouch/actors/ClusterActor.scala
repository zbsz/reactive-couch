package com.geteit.rcouch.actors

import akka.actor._
import com.geteit.rcouch.Settings.ClusterSettings
import akka.event.LoggingReceive
import scala.collection.mutable
import akka.pattern._
import com.geteit.rcouch.couchbase.Couchbase.{CouchbaseException, Bucket}
import scala.util.{Failure, Success}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import com.geteit.rcouch.actors.AdminActor.BucketNotFound
import akka.util.Timeout

/**
  */
class ClusterActor(config: ClusterSettings) extends Actor with ActorLogging {

  import ClusterActor._

  val buckets = new mutable.HashMap[String, ActorRef]

  val admin = context.actorOf(AdminActor.props(config))
  context.watch(admin)

  implicit val timeout = 15.seconds: Timeout
  implicit val ec: ExecutionContext = context.dispatcher

  def receive: Actor.Receive = LoggingReceive {
    case c: AdminActor.Command => admin.forward(c)
    case GetBucketActor(bucketName) =>
      buckets.get(bucketName) match {
        case Some(bucket) => sender ! bucket
        case None =>
          def get(retries: Int = 0, delay: Long = 100): Future[ActorRef] = {
            admin ? AdminActor.GetBucket(bucketName) flatMap {
              case b: Bucket =>
                Future.successful(buckets.getOrElseUpdate(bucketName, context.watch(context.actorOf(BucketActor.props(b, config)))))
              case BucketNotFound if retries < 5 =>
                after(delay.milliseconds, context.system.scheduler) { get(retries + 1, delay * 2) }
              case _ =>
                Future.failed(new CouchbaseException(s"Couldn't find bucket: $bucketName"))
            }
          }

          val s = sender
          get() onComplete {
            case Success(actor) => s ! actor
            case Failure(e) =>
              log.error(e, "GetBucketActor failed")
          }
      }
    case Terminated(actor) =>
      if (actor == admin) {
        log.error("Admin actor has been terminated, closing")
        context.stop(self)
      }
      buckets.find(_._2 == actor) match {
        case None =>
          log.error("Unknown child terminated, will close")
          context.stop(self)
        case Some((bucket, _)) =>
          log.warning(s"BucketActor terminated for: $bucket")
          buckets.remove(bucket)
          // XXX: should we restart it ???
      }
  }
}

object ClusterActor {

  sealed trait Command
  case class GetBucketActor(bucketName: String) extends Command

  def props(c: ClusterSettings) = Props(classOf[ClusterActor], c)
}
