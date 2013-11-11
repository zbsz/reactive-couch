package com.geteit.rcouch.actors

import akka.actor.Actor
import java.net.InetSocketAddress

/**
  */
class MemcachedCluster extends Actor {

  def receive: Actor.Receive = ???
}

object MemcachedCluster {

  sealed trait Command
  case class AddServer(address: InetSocketAddress)
}
