package com.geteit.rcouch.actors

import akka.actor._
import com.geteit.rcouch.Settings.ClusterSettings
import com.geteit.rcouch.actors.BucketMonitor.{Node, Bucket, Register}
import com.geteit.rcouch.memcached.{VBucketLocator, Memcached}
import scala.collection.immutable.Queue
import com.geteit.rcouch.Settings.ClusterSettings
import com.geteit.rcouch.actors.BucketMonitor.Bucket
import com.geteit.rcouch.actors.BucketMonitor.Node

/**
  */
class   ClusterActor(config: ClusterSettings) extends FSM[ClusterActor.State, ClusterActor.Data] with ActorLogging {

  import ClusterActor._

  val monitor = context.system.actorOf(BucketMonitor(config))
  context.watch(monitor)

  override def preStart(): Unit = {
    super.preStart()

    monitor ! Register
  }

  startWith(Initializing, InitData(Queue()))

  when(Initializing) {
    case Event(b: Bucket, InitData(queue)) =>
      NodesManager.update(b)
      if (NodesManager.initialized) goto(Running)
      else stay()
    case Event(msg, InitData(queue)) =>
      stay using InitData(queue.enqueue((sender, msg)))
  }

  when(Running) {
    case Event(b: Bucket, _) =>
      NodesManager.update(b)
      if (NodesManager.initialized) stay()
      else goto(Initializing) using InitData(Queue())
    case Event(c: Memcached.KeyCommand, _) =>
      NodesManager.primary(c.key).forward(c) // XXX: should we rotate to replicas?
      stay()
    case Event(c: ViewActor.Command, _) =>
      NodesManager.roundRobin.forward(c)
      stay()
  }

  onTransition {
    case Initializing -> Running =>
      stateData match {
        case InitData(queue) => queue foreach (p => self.tell(p._2, p._1))
        case d => log error s"Unexpected data: $d"
      }
  }

  initialize()

  object NodesManager {
    var nodes = Array[NodeRef]()
    var nodesMap = Map[String, NodeRef]()
    var locator: VBucketLocator = _
    var initialized = false
    var rrIndex = 0

    def update(b: Bucket): Unit = {
      locator = new VBucketLocator(b.vBucketServerMap)
      val map = b.nodes.map { n =>
        val host = n.hostname.substring(0, n.hostname.indexOf(':'))
          host -> nodesMap.getOrElse(host, createNodeActor(b, n))
      }.toMap
      nodesMap.filterNot(e => map.contains(e._1)).foreach(e => context.stop(e._2))
      nodesMap = map
      nodes = map.values.toArray
      initialized = !nodes.isEmpty && locator.hasVBuckets
    }

    def createNodeActor(b: Bucket, n: Node) = {
      val c = config.node.copy(memcached = config.node.memcached.copy(user = b.name, password = b.saslPasswd.getOrElse("")))
      context.system.actorOf(NodeActor(n, c))
    }
    
    def primary(key: String) = nodesMap(locator(key).primary)
    
    def roundRobin = {
      rrIndex = (rrIndex + 1) % nodes.length
      nodes(rrIndex)
    }
  }
}

object ClusterActor {
  type NodeRef = ActorRef

  sealed trait State
  case object Initializing extends State
  case object Running extends State

  sealed trait Data
  case class InitData(queue: Queue[(ActorRef, Any)]) extends Data

  def props(c: ClusterSettings) = Props(classOf[ClusterActor], c)
}
