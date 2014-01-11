package com.geteit.rcouch.memcached

import akka.util.ByteString
import java.util.concurrent.atomic.AtomicInteger

/**
  */
object Memcached {

  import BinaryPipeline._

  object Status {
    val NoError          = 0x0000.toShort
    val NotFound         = 0x0001.toShort
    val Exists           = 0x0002.toShort
    val ValueTooLarge    = 0x0003.toShort
    val InvalidArguments = 0x0004.toShort
    val NotStored        = 0x0005.toShort
    val NonNumeric       = 0x0006.toShort  // Incr/Decr on non-numeric value.
    val Unauthorized     = 0x0020.toShort  // Authentication required / Not Successful
    val ContinueAuth     = 0x0021.toShort	 // Further authentication steps required.
    val UnknownCommand   = 0x0081.toShort
    val OutOfMemory      = 0x0082.toShort
  }

  private val counter = new AtomicInteger(1)

  sealed abstract class Command(private[rcouch] val opcode: Byte, val opaque: Int = counter.getAndIncrement)

  sealed trait KeyCommand extends Command {
    val key: String
    var vBucket: Short = 0 // TODO: reimplement - mutable state
  }
  sealed trait GetCommand extends KeyCommand
  sealed trait QuietCommand extends Command

  case class Get(key: String) extends Command(Opcode.Get) with GetCommand
  case class GetK(key: String) extends Command(Opcode.GetK) with GetCommand
  case class GetQ(key: String) extends Command(Opcode.GetQ) with GetCommand with QuietCommand
  case class GetKQ(key: String) extends Command(Opcode.GetKQ) with GetCommand with QuietCommand


  sealed trait StoreCommand extends KeyCommand {
    val value: ByteString
    val expiry: Int
    val flags: Int
    val cas: Long
  }

  case class Set(key: String, value: ByteString, flags: Int, expiry: Int, cas: Long = 0) extends Command(Opcode.Set) with StoreCommand
  case class SetQ(key: String, value: ByteString, flags: Int, expiry: Int, cas: Long = 0) extends Command(Opcode.SetQ) with StoreCommand with QuietCommand
  case class Add(key: String, value: ByteString, flags: Int, expiry: Int, cas: Long = 0) extends Command(Opcode.Add) with StoreCommand
  case class AddQ(key: String, value: ByteString, flags: Int, expiry: Int, cas: Long = 0) extends Command(Opcode.AddQ) with StoreCommand with QuietCommand
  case class Replace(key: String, value: ByteString, flags: Int, expiry: Int, cas: Long = 0) extends Command(Opcode.Replace) with StoreCommand
  case class ReplaceQ(key: String, value: ByteString, flags: Int, expiry: Int, cas: Long = 0) extends Command(Opcode.ReplaceQ) with StoreCommand with QuietCommand

  sealed trait DeleteCommand extends KeyCommand
  case class Delete(key: String) extends Command(Opcode.Delete) with DeleteCommand
  case class DeleteQ(key: String) extends Command(Opcode.DeleteQ) with DeleteCommand with QuietCommand

  sealed trait CounterCommand extends KeyCommand {
    val delta: Long
    val initial: Long
    val expiry: Int
  }
  case class Incr(key: String, delta: Long, initial: Long, expiry: Int) extends Command(Opcode.Increment) with CounterCommand
  case class IncrQ(key: String, delta: Long, initial: Long, expiry: Int) extends Command(Opcode.IncrementQ) with CounterCommand with QuietCommand
  case class Decr(key: String, delta: Long, initial: Long, expiry: Int) extends Command(Opcode.Decrement) with CounterCommand
  case class DecrQ(key: String, delta: Long, initial: Long, expiry: Int) extends Command(Opcode.DecrementQ) with CounterCommand with QuietCommand

  sealed trait ModStringCommand extends KeyCommand {
    val value: String
  }
  case class Append(key: String, value: String) extends Command(Opcode.Append) with ModStringCommand
  case class AppendQ(key: String, value: String) extends Command(Opcode.AppendQ) with ModStringCommand with QuietCommand
  case class Prepend(key: String, value: String) extends Command(Opcode.Prepend) with ModStringCommand
  case class PrependQ(key: String, value: String) extends Command(Opcode.PrependQ) with ModStringCommand with QuietCommand


  sealed trait QuitCommand extends Command
  case class Quit() extends Command(Opcode.Quit) with QuitCommand
  case class QuitQ() extends Command(Opcode.QuitQ) with QuitCommand with QuietCommand

  sealed trait FlushCommand extends Command {
    val expiry: Int
  }
  case class Flush(expiry: Int) extends Command(Opcode.Flush) with FlushCommand
  case class FlushQ(expiry: Int) extends Command(Opcode.FlushQ) with FlushCommand with QuietCommand

  case class NoOp() extends Command(Opcode.NoOp)
  case class Version() extends Command(Opcode.Version)

  case class Stat(key: Option[String]) extends Command(Opcode.Stat)

  sealed trait AuthCommand extends Command
  sealed trait AuthRequest extends AuthCommand

  case class AuthList() extends Command(Opcode.AuthList) with AuthCommand
  case class AuthReqPlain(user: String, passwd: String) extends Command(Opcode.AuthStart) with AuthRequest
  case class AuthStep() extends Command(Opcode.AuthStep) with AuthCommand


  sealed trait Response {
    val opcode: Byte
    val status: Short
    val opaque: Int
  }

  case class GetResponse(opcode: Byte, key: Option[String], value: ByteString, flags: Int, cas: Long, status: Short, opaque: Int) extends Response {
    override def toString: String = s"GetResponse(key=$key, value=${value.decodeString("utf8")}, status=$status, opaque=$opaque)"
  }
  case class StoreResponse(opcode: Byte, cas: Long, status: Short, opaque: Int) extends Response
  case class CounterResponse(opcode: Byte, status: Short, value: Long, cas: Long, opaque: Int) extends Response
  case class StatusResponse(opcode: Byte, status: Short, opaque: Int) extends Response
  case class VersionResponse(version: String, status: Short, opaque: Int) extends Response { val opcode = Opcode.Version }
  case class StatResponse(key: Option[String], value: Option[String], status: Short, opaque: Int) extends Response { val opcode = Opcode.Stat }

  case class AuthListResponse(mechanisms: String, status: Short = 0, opaque: Int) extends Response { val opcode = Opcode.AuthList }
  case class AuthReqResponse(msg: Option[String], status: Short = 0, opaque: Int) extends Response { val opcode = Opcode.AuthList }

  object Response {
    def unapply(r: Response) = Some(r.status)
  }
  object ErrorResponse {
    def unapply(r: Response) = if (r.status == Status.NoError) None else Some(r.status)
  }
}

