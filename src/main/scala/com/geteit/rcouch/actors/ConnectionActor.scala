package com.geteit.rcouch.actors

import akka.actor.{ActorRef, ActorLogging, Actor}
import com.geteit.rcouch.Settings.ConnectionSettings
import java.net.InetSocketAddress
import akka.io.{IO, Tcp}
import scala.concurrent.duration._

/**
  */
private abstract class ConnectionActor(address: InetSocketAddress, config: ConnectionSettings) extends Actor with ActorLogging {

  import Tcp._
  import context.system
  import context.dispatcher

  /**
   * The initial amount of time that the client will wait before trying
   * to reconnect to the server after losing a connection
   */
  private var reconnectDelayMillis = 1000

  /**
   * The number of consecutive failed attempts to connect to the server
   */
  private var reconnectAttempts = config.maxReconnectAttempts


  IO(Tcp) ! Connect(address)

  def receive: Actor.Receive = {
    case CommandFailed(_: Connect) => attemptReconnect()
    case Connected(remote, local) =>
      resetReconnectCounters()
      val connection = sender
      connection ! Register(self)

      context become connected(connection).orElse {
        case _: ConnectionClosed =>
          context unbecome()
          attemptReconnect()
      }
  }

  protected def connected(connection: ActorRef): Actor.Receive


  /**
   * Attempts to reconnect to server after having lost the connection.
   */
  private def attemptReconnect() {
    if (reconnectAttempts >= config.maxReconnectAttempts)
      throw new ConnectionException(s"Cannot connect to $address after $reconnectAttempts failed attempts. Abandoning this connection")

    log warning "Connection to " + address + " lost. Will retry in " + reconnectDelayMillis + " ms."
    system.scheduler.scheduleOnce(reconnectDelayMillis.milliseconds) { IO(Tcp) ! Connect(address) }
    reconnectDelayMillis = math.min(reconnectDelayMillis * 2, config.maxReconnectDelayMillis)
    reconnectAttempts += 1
  }

  /**
   * Resets reconnectAttempts and reconnectDelayMillis to their default values
   */
  private def resetReconnectCounters() = {
    if (reconnectAttempts > 0) {
      log warning s"Connection to $address re-established"
      reconnectAttempts = 0
      reconnectDelayMillis = 1000
    }
  }
}
