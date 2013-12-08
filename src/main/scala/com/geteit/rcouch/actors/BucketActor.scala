package com.geteit.rcouch.actors

import akka.actor._
import com.geteit.rcouch.Settings.ClusterSettings
import com.geteit.rcouch.memcached.{Memcached, VBucketLocator}
import akka.event.LoggingReceive
import com.geteit.rcouch.couchbase.Couchbase.Bucket
import com.geteit.rcouch.couchbase.Couchbase.Node

/**
  */
class BucketActor(bucket: Bucket, config: ClusterSettings) extends Actor with Stash with ActorLogging {

  NodesManager.update(bucket)
  
  val monitor = context.actorOf(BucketMonitor.props(bucket, config))
  context.watch(monitor)

  def receive: Actor.Receive = watchChildren orElse {
    case b: Bucket =>
      NodesManager.update(b)
      if (NodesManager.initialized) {
        context.become(running)
        unstashAll()
      }
    case msg => stash()
  }
  
  def running: Actor.Receive = watchChildren orElse LoggingReceive {
    case b: Bucket =>
      NodesManager.update(b)
      if (!NodesManager.initialized) context.unbecome() 
    case c: Memcached.KeyCommand =>
      NodesManager.primary(c.key).forward(c) // XXX: should we rotate to replicas?
    case c: ViewActor.Command =>
      NodesManager.roundRobin.forward(c)
    case c: AdminActor.Command =>
      context.parent.forward(c)
  }
  
  def watchChildren: Actor.Receive = {
    case Terminated(m) if m == monitor =>
      log.error(s"BucketMonitor has been terminated for ${bucket.name}, will stop bucket actor")
      context.stop(self)
    case Terminated(node) =>
      NodesManager.nodeTerminated(node)
  } 

  object NodesManager {

    var nodes = Array[NodeRef]()
    var nodesMap = Map[String, NodeRef]()
    var locator: VBucketLocator = _
    var initialized = false
    var rrIndex = 0

    def update(b: Bucket): Unit = {
      locator = new VBucketLocator(b.vBucketServerMap)
      val map = b.nodes.filter(_.healthy).map { n =>
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

    def nodeTerminated(node: ActorRef): Unit = {
      // TODO: implement node watch, should we restart it or remove from list ??
    }
    
    def primary(key: String) = nodesMap(locator(key).primary)

    def roundRobin = {
      rrIndex = (rrIndex + 1) % nodes.length
      nodes(rrIndex)
    }
  }
}

object BucketActor {
  type BucketActorRef = ActorRef

  def props(bucket: Bucket, config: ClusterSettings) = Props(classOf[BucketActor], bucket, config)
}
