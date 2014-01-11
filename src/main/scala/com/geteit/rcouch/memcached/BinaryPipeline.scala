package com.geteit.rcouch.memcached

import akka.util.ByteString
import akka.io._
import java.nio.ByteOrder
import scala.annotation.tailrec
import scala.Some
import com.geteit.rcouch.memcached.Memcached._
import akka.event.Logging

/**
 * Binary memcached protocol implementation.
 *
 * @see http://code.google.com/p/memcached/wiki/MemcacheBinaryProtocol
 * @see http://code.google.com/p/memcached/wiki/SASLAuthProtocol
 * @see http://docs.couchbase.com/couchbase-devguide-2.2/#providing-sasl-authentication
 */
object BinaryPipeline {

  case class Frame(opcode: Byte, key: Option[ByteString], value: Option[ByteString] = None, extras: Option[ByteString] = None, status: Short = 0, cas: Long = 0, opaque: Int = 0) {

    require(extras == None || extras.get.length < 256, "Extras length should be smaller than 256")

    def totalBodyLen = extras.fold(0)(_.length) + key.fold(0)(_.length) + value.fold(0)(_.length)
  }

  val RequestMagic = 0x80.toByte
  val ResponseMagic = 0x81.toByte

  val HeaderLen = 24

  object Opcode {
    val Get = 0x00.toByte
    val Set = 0x01.toByte
    val Add = 0x02.toByte
    val Replace = 0x03.toByte
    val Delete = 0x04.toByte
    val Increment = 0x05.toByte
    val Decrement = 0x06.toByte
    val Quit = 0x07.toByte
    val Flush = 0x08.toByte
    val GetQ = 0x09.toByte
    val NoOp = 0x0A.toByte
    val Version = 0x0B.toByte
    val GetK = 0x0C.toByte
    val GetKQ = 0x0D.toByte
    val Append = 0x0E.toByte
    val Prepend = 0x0F.toByte
    val Stat = 0x10.toByte
    val SetQ = 0x11.toByte
    val AddQ = 0x12.toByte
    val ReplaceQ = 0x13.toByte
    val DeleteQ = 0x14.toByte
    val IncrementQ = 0x15.toByte
    val DecrementQ = 0x16.toByte
    val QuitQ = 0x17.toByte
    val FlushQ = 0x18.toByte
    val AppendQ = 0x19.toByte
    val PrependQ = 0x1A.toByte

    val AuthList = 0x20.toByte
    val AuthStart = 0x21.toByte
    val AuthStep = 0x22.toByte
  }

  class FormatException(msg: String) extends Exception(msg)

  def apply() = new MemcachedMessageStage >> new MemcachedFrameStage

  implicit val byteOrder = ByteOrder.BIG_ENDIAN
}

object MemcachedFrameStage {

  import BinaryPipeline._

  @tailrec private[memcached] def extractFrames(bs: ByteString, acc: List[Frame]): (ByteString, List[Frame]) = {
    if (bs.length < HeaderLen) (bs, acc)
    else {
      val totalBodyLen = bs.iterator.drop(8).getInt
      if (bs.length >= HeaderLen + totalBodyLen) {
        extractFrames(bs.drop(HeaderLen + totalBodyLen), decodeFrame(bs) :: acc)
      } else (bs, acc)
    }
  }

  private[memcached] def decodeFrame(bs: ByteString) = {
    val iter = bs.iterator
    val magic = iter.getByte
    if (magic != ResponseMagic) throw new FormatException(s"Incorrect response magic: $magic")
    val opcode = iter.getByte
    val keyLen = iter.getShort
    val extrasLen = 0xff & iter.getByte.asInstanceOf[Int]
    iter.drop(1) // data type
    val status = iter.getShort
    val totalBodyLen = iter.getInt
    val opaque = iter.getInt
    val cas = iter.getLong
    val extras = if (extrasLen <= 0) None else Some(bs.slice(HeaderLen, HeaderLen + extrasLen))
    val key = if (keyLen <= 0) None else Some(bs.slice(HeaderLen + extrasLen, HeaderLen + extrasLen + keyLen))
    val valueLen = totalBodyLen - keyLen - extrasLen
    val value = if (valueLen <= 0) None else Some(bs.slice(HeaderLen + extrasLen + keyLen, HeaderLen + extrasLen + keyLen + valueLen))

    Frame(opcode, key, value, extras, status, cas, opaque)
  }
}

class MemcachedFrameStage extends SymmetricPipelineStage[PipelineContext, BinaryPipeline.Frame, ByteString] {

  import BinaryPipeline._
  import MemcachedFrameStage._

  def apply(ctx: PipelineContext) = new SymmetricPipePair[Frame, ByteString] {
    override def commandPipeline = { frame =>
      val sb = ByteString.newBuilder
      sb.putByte(RequestMagic)
      sb.putByte(frame.opcode)
      sb.putShort(frame.key.fold(0)(_.length))
      sb.putByte(frame.extras.fold(0)(_.length).toByte)
      sb.putByte(0) // DataType
      sb.putShort(frame.status) // Reserved - vBucket / Status
      sb.putInt(frame.totalBodyLen)
      sb.putInt(frame.opaque)
      sb.putLong(frame.cas)
      frame.extras foreach sb.append
      frame.key foreach sb.append
      frame.value foreach sb.append

      ctx.singleCommand(sb.result())
    }

    var buffer = ByteString()

    override def eventPipeline = { bs =>
      val data = if (buffer.isEmpty) bs else buffer ++ bs
      val (nb, frames) = extractFrames(data, Nil)
      buffer = nb

      frames match {
        case Nil        ⇒ Nil
        case one :: Nil ⇒ ctx.singleEvent(one)
        case many       ⇒ many reverseMap (Left(_))
      }
    }
  }
}

object MemcachedMessageStage {

  import BinaryPipeline._

  private val GetBuilder = { f: Frame => GetResponse(f.opcode, f.key.map(_.decodeString("utf8")), f.value.getOrElse(ByteString.empty), f.extras.fold(0)(_.iterator.getInt), f.cas, f.status, f.opaque) }

  private val StoreBuilder = { f: Frame => StoreResponse(f.opcode, f.cas, f.status, f.opaque) }

  private val StatusBuilder = { f: Frame => StatusResponse(f.opcode, f.status, f.opaque) }

  private val CounterBuilder = { f: Frame => CounterResponse(f.opcode, f.status, f.value.fold(0L)(_.iterator.getLong), f.cas, f.opaque) }

  private val ResponseBuilders = Map[Byte, Frame => Response](
    Opcode.Get -> GetBuilder,
    Opcode.GetK -> GetBuilder,
    Opcode.GetQ -> GetBuilder,
    Opcode.GetKQ -> GetBuilder,
    Opcode.Set -> StoreBuilder,
    Opcode.SetQ -> StoreBuilder,
    Opcode.Add -> StoreBuilder,
    Opcode.AddQ -> StoreBuilder,
    Opcode.Replace -> StoreBuilder,
    Opcode.ReplaceQ -> StoreBuilder,
    Opcode.Increment -> CounterBuilder,
    Opcode.IncrementQ -> CounterBuilder,
    Opcode.Decrement -> CounterBuilder,
    Opcode.DecrementQ -> CounterBuilder,
    Opcode.NoOp -> StatusBuilder,
    Opcode.Delete -> StatusBuilder,
    Opcode.DeleteQ -> StatusBuilder,
    Opcode.Quit -> StatusBuilder,
    Opcode.QuitQ -> StatusBuilder,
    Opcode.Flush -> StatusBuilder,
    Opcode.FlushQ -> StatusBuilder,
    Opcode.Append -> StatusBuilder,
    Opcode.AppendQ -> StatusBuilder,
    Opcode.Prepend -> StatusBuilder,
    Opcode.PrependQ -> StatusBuilder,
    Opcode.Stat -> { f: Frame => StatResponse(f.key.map(_.decodeString("utf8")), f.value.map(_.decodeString("utf8")), f.status, f.opaque) },
    Opcode.Version -> {f => VersionResponse(f.value.fold("")(_.decodeString("utf8")), f.status, f.opaque)},
    Opcode.AuthList -> {f => AuthListResponse(f.value.fold("")(_.decodeString("utf8")), f.status, f.opaque)},
    Opcode.AuthStart -> {f => AuthReqResponse(f.value.map(_.decodeString("utf8")), f.status, f.opaque)}
  )

  val Plain = ByteString("PLAIN", "utf8")
}

class MemcachedMessageStage extends PipelineStage[HasActorContext, Command, BinaryPipeline.Frame, Response, BinaryPipeline.Frame] {

  import MemcachedMessageStage._
  import BinaryPipeline._

  def apply(ctx: HasActorContext) = new PipePair[Command, Frame, Response, Frame] {

    private val log = Logging(ctx.getContext.system, classOf[MemcachedMessageStage])

    def commandPipeline = { cmd =>
      log debug s"Encoding command: $cmd"

      def frame(key: String, value: Option[ByteString] = None, extras: Option[ByteString] = None, cas: Long = 0, vBucket: Short = 0) =
        new Frame(cmd.opcode, Some(ByteString(key, "utf8")), value, extras, cas = cas, opaque = cmd.opaque, status = vBucket)

      def frame2(value: Option[ByteString] = None, extras: Option[ByteString] = None, cas: Long = 0) =
        new Frame(cmd.opcode, None, value, extras, cas = cas, opaque = cmd.opaque)

      ctx.singleCommand(cmd match {
        case _: QuitCommand      => frame2()
        case _: AuthList         => frame2()
        case _: Version          => frame2()
        case _: AuthStep         => frame2()
        case _: NoOp             => frame2()
        case Stat(key)           => key.fold(frame2())(frame(_))
        case g: GetCommand       => frame(g.key, vBucket = g.vBucket)
        case d: DeleteCommand    => frame(d.key, vBucket = d.vBucket)
        case s: StoreCommand     => frame(s.key, Some(s.value), Some(ByteString.newBuilder.putInt(s.flags).putInt(s.expiry).result()), s.cas, vBucket = s.vBucket)
        case c: CounterCommand   => frame(c.key, extras = Some(ByteString.newBuilder.putLong(c.delta).putLong(c.initial).putInt(c.expiry).result()), vBucket = c.vBucket)
        case m: ModStringCommand => frame(m.key, Some(ByteString(m.value, "utf8")), vBucket = m.vBucket)
        case f: FlushCommand     => frame2(extras = if (f.expiry > 0) Some(ByteString.newBuilder.putInt(f.expiry).result()) else None)
        case r: AuthReqPlain     => Frame(r.opcode, Some(Plain), Some(ByteString.newBuilder.putByte(0).append(ByteString(r.user, "utf8")).putByte(0).append(ByteString(r.passwd, "utf8")).result()), opaque = r.opaque)
      })
    }

    def eventPipeline = { frame =>
      ctx.singleEvent(ResponseBuilders(frame.opcode)(frame))
    }
  }
}
