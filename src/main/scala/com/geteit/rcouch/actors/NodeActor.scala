package com.geteit.rcouch.actors

import akka.actor.{Props, Actor, ActorLogging}
import com.geteit.rcouch.Settings.NodeConfig
import java.net.InetSocketAddress
import com.geteit.rcouch.memcached.Memcached
import com.geteit.rcouch.actors.NodeActor.MemcachedAddress
import com.geteit.rcouch.couchbase.Couchbase.Node


/**
 * Single couchbase node.
 */
class NodeActor(node: Node, config: NodeConfig) extends Actor with ActorLogging {

  import context._

  val memcached = {
    val MemcachedAddress(address) = node
    system.actorOf(MemcachedIo.props(address, self, config.memcached))
  }

  val view = system.actorOf(ViewActor.props(node.couchApiBase))

  override def preStart(): Unit = {
    super.preStart()
  }

  def receive: Actor.Receive = {
    case c: Memcached.Command => memcached.forward(c)
    case c: ViewActor.Command => view.forward(c)
  }
}

object NodeActor {

  def apply(n: Node, c: NodeConfig) = Props.create(classOf[NodeActor], n, c)

  object MemcachedAddress {

    def unapply(n: Node): Option[InetSocketAddress] = {
      val i = n.hostname.indexOf(':')
      val host = if (i > 0) n.hostname.substring(0, i) else n.hostname
      Some(new InetSocketAddress(host, n.ports.direct))
    }
  }

}
