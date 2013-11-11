package com.geteit.rcouch.actors

import java.net.InetSocketAddress
import com.geteit.rcouch.memcached.Memcached._
import akka.util.ByteString
import akka.io._
import akka.io.Tcp._
import scala.collection.immutable.Queue
import com.geteit.rcouch.memcached.Memcached.Command
import scala.concurrent.duration._
import com.geteit.rcouch.memcached.BinaryPipeline
import akka.actor._
import akka.io.IO
import com.geteit.rcouch.memcached.Memcached.Status._
import scala.util.Failure
import scala.Some
import com.geteit.rcouch.memcached.Memcached.AuthListResponse
import scala.util.Success
import akka.io.Tcp.Received
import com.geteit.rcouch.memcached.Memcached.AuthReqPlain
import com.geteit.rcouch.memcached.Memcached.AuthReqResponse
import akka.io.Tcp.Connected
import com.geteit.rcouch.memcached.Memcached.NoOp
import akka.io.Tcp.Register
import akka.io.Tcp.Connect
import akka.io.Tcp.CommandFailed
import com.geteit.rcouch.Config.MemcachedConfig
import com.geteit.rcouch.memcached.Memcached.AuthList
import com.geteit.rcouch.actors.MemcachedIo.{RunningData, CommandData}

object MemcachedIo {

  case class Reject(cmd: Command, cause: String)

  case class CommandData(cmd: Command, sender: ActorRef)

  type Pipe = PipelineInjector[Command, ByteString]

  sealed trait Data {
    val queue: Queue[CommandData]

    def updated(queue: Queue[CommandData]): Data
  }
  trait PipelineData {
    val pipeline: Pipe
  }
  case class ConnectingData(address: InetSocketAddress, retriesLeft: Int = 0, retryDelay: Int = 1000, queue: Queue[CommandData] = Queue.empty[CommandData]) extends Data {
    override def updated(queue: Queue[CommandData]) = copy(queue = queue)
  }
  case class AuthorizingData(retriesLeft: Int = 0, pipeline: Pipe, queue: Queue[CommandData]) extends Data with PipelineData {
    override def updated(queue: Queue[CommandData]) = copy(queue = queue)
  }
  case class RunningData(pipeline: Pipe, queue: Queue[CommandData], cmds: List[CommandData] = Nil) extends Data with PipelineData {
    override def updated(queue: Queue[CommandData]) = copy(queue = queue)
  }
  case class FailedData(queue: Queue[CommandData], cmds: List[CommandData] = Nil) extends Data {
    override def updated(queue: Queue[CommandData]) = copy(queue = queue)
  }

  object QueueData {
    def unapply(d: Data) = Some(d.queue)
  }

  object PipelineData {
    def unapply(d: Data): Option[Pipe] = d match {
      case p: PipelineData => Some(p.pipeline)
      case _ => None
    }
  }


  sealed trait State

  case object Connecting extends State
  case object Authorizing extends State
  case object Failed extends State
  case object Running extends State


  def props(address: InetSocketAddress, node: NodeRef, config: MemcachedConfig) = Props(classOf[MemcachedIo], address, node, config)
}

/**
 * Actor responsible for memcached communication to and from a single node using a single connection.
 */
private class MemcachedIo(val address: InetSocketAddress, val node: NodeRef, val config: MemcachedConfig)
    extends Actor with FSM[MemcachedIo.State, MemcachedIo.Data] with UnhandledFSM with ConnectingFSM
    with MemcachedAuthorizer with MemcachedRunning with MemcachedBackPressure with ActorLogging {

  import MemcachedIo._

  protected var connection: ActorRef = _

  onConnected {
    case (connection: ActorRef, ConnectingData(_, _, _, queue)) => {
      this.connection = connection
      val ctx = new HasActorContext {
        override def getContext = context
      }

      val pipeline = PipelineFactory.buildWithSinkFunctions(ctx, BinaryPipeline())({
        case Success(frame) =>
          log debug "Sending command: " + frame.decodeString("utf8")

          self ! Write(frame)
        case Failure(ex) => log.error(ex, "couldn't encode memcached command")
      }, {
        case Success(res) =>
          log debug s"Decoded response: $res"
          self ! res
        case Failure(ex) => log.error(ex, "couldn't decode memcached response")
      })
      if (config.authEnabled) goto(Authorizing) using AuthorizingData(pipeline = pipeline, queue = queue)
      else goto(Running) using RunningData(pipeline, queue)
    }
  }

  startWith(Connecting, ConnectingData(address))

  onTransition {
    case _ -> Failed =>
      // TODO: should probably send commands to current Node so they get retried on different connection
      //      queue foreach (_.sender ! ErrorResponse())
      //      queue = Queue.empty
      throw new ConnectionException("Memcached client failed")
  }


  override def preStart() {
    log debug s"MemcachedIo starting on $address"
    super.preStart()

    initialize()
  }
}

private trait UnhandledFSM extends FSM[MemcachedIo.State, MemcachedIo.Data]{
  private var defHandlers = PartialFunction.empty: StateFunction

  def onUnhandled(handler: StateFunction): Unit = {
    defHandlers = handler orElse defHandlers
  }

  override def preStart() {
    whenUnhandled(defHandlers)
    super.preStart()
  }
}

private trait ConnectingFSM {
  _: MemcachedIo =>

  import MemcachedIo._
  import context._

  val address: InetSocketAddress
  val config: MemcachedConfig

  type OnConnected = PartialFunction[(ActorRef, ConnectingData), State]
  private var onConnected: OnConnected = {
    case _ =>
      log error s"No onConnected handler was registered."
      stay()
  }

  protected def onConnected(handler: OnConnected): Unit = {
    onConnected = handler orElse onConnected
  }

  onUnhandled {
    case Event(_: ConnectionClosed, data) =>
      data match {
        case RunningData(_, _, cmds) => // TODO: Reject pending commands
        case _ =>
      }
      goto(Connecting) using ConnectingData(address, config.connection.maxReconnectAttempts, 1000, data.queue)
    case _ =>
      log warning s"Running Connecting onUnhandled"
      stay()
  }

  when(Connecting) {
    case Event(CommandFailed(_: Connect), ConnectingData(address, retriesLeft, retryDelay, queue)) =>
      if (retriesLeft <= 0) {
        log error s"Cannot connect to $address. Abandoning this connection"
        goto(Failed) using FailedData(queue = queue)
      } else {
        log warning s"Connection to $address lost. Will retry in $retryDelay ms."
        import context._
        system.scheduler.scheduleOnce(retryDelay.milliseconds)(IO(Tcp) ! Connect(address))
        stay using ConnectingData(address, retriesLeft - 1, math.min(retryDelay * 2, 16000), queue)
      }

    case Event(Connected(remote, local), data @ ConnectingData(address, _, _, _)) =>
      log info s"Connection to $address established"
      stay()
      val connection = sender
      connection ! Register(self)
      onConnected(connection, data)
  }

  onTransition {
    case _ -> Connecting =>
      nextStateData match {
        case ConnectingData(address, _, _, _) => IO(Tcp) ! Connect(address)
        case d => throw new IllegalStateException(s"Unexpected data in transition to Connecting state: $d")
      }
  }

  IO(Tcp) ! Connect(address)
}

private trait MemcachedAuthorizer extends FSM[MemcachedIo.State, MemcachedIo.Data] {

  import MemcachedIo._

  val config: MemcachedConfig

  when(Authorizing) {
    case Event(AuthListResponse(_, NoError, _), AuthorizingData(_, pipeline, _)) =>
      pipeline.injectCommand(AuthReqPlain(config.user, config.password))
      stay()
    case Event(ErrorResponse(UnknownCommand) | AuthReqResponse(_, NoError, _),  AuthorizingData(_, pipeline, queue)) => // TODO: should probably check opaque of UnknownCommand response
      goto(Running) using RunningData(pipeline, queue)
  }

  onTransition {
    case _ -> Authorizing =>
      nextStateData match {
        case PipelineData(pipe) => pipe.injectCommand(AuthList())
        case _ => throw new IllegalStateException("No pipeline when going to Authorizing state.")
      }
  }
}

private trait MemcachedRunning {
  _: MemcachedIo =>

  import MemcachedIo._

  val config: MemcachedConfig

  when(Running) {
    case Event(cmd: Command, data @ RunningData(pipeline, queue, cmds)) =>
      enqueue(cmd, queue).fold {
        stay()
      } { q =>
        stay using checkQueue(pipeline, q, cmds)
      }
    case Event(res: Response, RunningData(pipeline, queue, cmds)) =>
      val i = cmds.indexWhere(_.cmd.opaque == res.opaque)
      if (i < 0) {
        log warning s"Received Memcached response $res has no corresponding command in: $cmds"
        stay()
      } else {
        val d = cmds(i)
        d.sender ! res

        log info s"Received Memcached response $res for command: ${d.cmd}"
        if (i > 0) log debug s"Will drop ${i + 1} commands ${cmds.take(i + 1)}"

        stay using RunningData(pipeline, queue, cmds.drop(i + 1))
      }
  }

  onUnhandled {
    case Event(Received(raw: ByteString), PipelineData(pipeline)) =>
      log debug s"got reponse: $raw"
      pipeline.injectEvent(raw)
      stay()
    case Event(cmd: Command, data @ QueueData(queue)) =>
      enqueue(cmd, queue).fold(stay())(q => stay using data.updated(q))
    case _ =>
      log warning s"Running CACHED onUnhandled"
      stay()
  }

  onTransition {
    case _ -> Running => self ! NoOp()
  }

  def enqueue(cmd: Command, queue: Queue[CommandData]) = {
    log debug s"enqueue command: $cmd"
    if (queue.length < config.maxQueueSize) {
      val cd = CommandData(cmd, sender)
      Some(queue.enqueue(cd))
    } else {
      sender ! Reject(cmd, "command queue is full")
      None
    }
  }

  def checkQueue(pipeline: Pipe, queue: Queue[CommandData], cmds: List[CommandData]): RunningData = {
    log debug s"check queue: $queue"
    if (queue.isEmpty) RunningData(pipeline, queue, cmds)
    else {
      val cd = queue.head
      pipeline.injectCommand(cd.cmd) //TODO: implement throttling, use NACK based approach
      checkQueue(pipeline, queue.tail, cd :: cmds)
    }
  }
}

private trait MemcachedBackPressure {
  _: MemcachedIo with UnhandledFSM with MemcachedRunning =>

  case class Ack() extends Tcp.Event

  private var suspended = false
  private var writeBuffer = Queue[ByteString]()
  private var ackBuffer = Queue[ByteString]()
  private var retryBuffer = Queue[ByteString]()

  override def checkQueue(pipeline: MemcachedIo.Pipe, queue: Queue[CommandData], cmds: List[CommandData]): RunningData = {
    log debug s"check queue: $queue"
    if (queue.isEmpty || suspended) RunningData(pipeline, queue, cmds)
    else {
      val cd = queue.head
      pipeline.injectCommand(cd.cmd)
      checkQueue(pipeline, queue.tail, cd :: cmds)
    }
  }

  onUnhandled {
    case Event(Write(s, _), _) =>
      if (suspended) {
        writeBuffer = writeBuffer.enqueue[ByteString](s)
      } else {
        ackBuffer = ackBuffer.enqueue[ByteString](s)
        connection ! Write(s, Ack())
      }
      stay()
    case Event(Ack(), data) =>
      ackBuffer = ackBuffer.tail
      if (suspended) {
        retry()
      }
      stay()
    case Event(CommandFailed(Write(_, Ack())), _) =>
      if (!suspended) {
        log info s"Write failed, suspend = true"
        suspended = true
        retryBuffer = ackBuffer
        connection ! ResumeWriting
      }
      stay()
    case Event(WritingResumed, _) =>
      retry()
      stay()
  }

  private def retry(): Unit = {
      if (retryBuffer.isEmpty) {
        log info s"Resent all data, suspend = false"
        suspended = false
        writeBuffer foreach { s =>
          connection ! Write(s)
          ackBuffer = ackBuffer.enqueue[ByteString](s)
        }
        writeBuffer = Queue()
      } else {
        connection ! Write(retryBuffer.head)
        retryBuffer = retryBuffer.tail
      }
  }
}
