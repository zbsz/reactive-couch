package com.geteit.rcouch

import akka.actor.ActorRef

/**
  */
package object actors {

  type NodeRef = ActorRef


  class ConnectionException(msg: String) extends Exception(msg)
}
