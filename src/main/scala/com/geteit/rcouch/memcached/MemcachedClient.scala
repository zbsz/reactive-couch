package com.geteit.rcouch.memcached

import com.geteit.rcouch.Client
import akka.util.{Timeout, ByteString}
import spray.json.{JsonParser, JsValue}
import concurrent.{ExecutionContext, Future}
import concurrent.duration._
import akka.pattern._

/**
  */
trait MemcachedClient extends Client {
  import Memcached._

  def get[A: Transcoder](key: String): Future[Option[A]] =
    ask(actor, Memcached.Get(key)).mapTo[GetResponse].map { res =>
      res.status match {
        case Status.NoError => Option(res.value).map(implicitly[Transcoder[A]].decode)
        case Status.NotFound => None
        case _ => throw new IllegalStateException(s"Unexpected response for get operation: $res")
      }
    }

  def gets[A: Transcoder](key: String): Future[Option[CasValue[A]]] =
    ask(actor, Memcached.Get(key)).mapTo[GetResponse].map { res =>
      res.status match {
        case Status.NoError => Option(res.value).map(str => CasValue(implicitly[Transcoder[A]].decode(str), CasId(res.cas)))
        case Status.NotFound => None
        case _ => throw new IllegalStateException(s"Unexpected response for get operation: $res")
      }
    }

  def add[A: Transcoder](key: String, value: A, exp: Expire = Expire.Never): Future[Boolean] =
    storeOp(value, Add(key, _, 0, exp.time))

  def replace[A: Transcoder](key: String, value: A, exp: Expire): Future[Boolean] =
    storeOp(value, Replace(key, _, 0, exp.time))

  def set[A: Transcoder](key: String, value: A, exp: Expire): Future[Boolean] =
    storeOp(value, Set(key, _, 0, exp.time))

  private def storeOp[A: Transcoder](value: A, f: ByteString => StoreCommand): Future[Boolean] =
    implicitly[Transcoder[A]].withEncoded(value){ v =>
      ask(actor, f(v)).mapTo[Response].map(_.status == Status.NoError)
    }

  def cas[A: Transcoder](key: String, value: A, cas: CasId, exp: Expire = Expire.Never): Future[CasResponse] =
    implicitly[Transcoder[A]].withEncoded(value){ v =>
      ask(actor, Set(key, v, 0, exp.time, cas = cas.id)).mapTo[StoreResponse].map { res =>
        res.status match {
          case Status.NoError => CasResponse.Ok
          case Status.Exists => CasResponse.Exists
          case Status.NotFound => CasResponse.NotFound
          case _ => throw new IllegalStateException(s"Unexpected response for cas operation: $res")
        }
      }
    }

  def delete(key: String): Future[Boolean] =
    ask(actor, Delete(key)).mapTo[Response].map(_.status == Status.NoError)
}

case class CasValue[A](v: A, cas: CasId)

sealed trait CasResponse
object CasResponse {
  case object Ok extends CasResponse
  case object Exists extends CasResponse
  case object NotFound extends CasResponse
}

sealed case class CasId(id: Long)
object CasId {
  val Unspecified = CasId(0)
}

sealed trait Expire {
  val time: Int
}
object Expire {
  private val MaxDelaySeconds = 30 * 24 * 3600 //30 days
  
  case object Never extends Expire { override val time = 0 }
  
  def After(seconds: Int): Expire = new Expire {
    override val time = if (seconds < MaxDelaySeconds) seconds else (System.currentTimeMillis() / 1000).toInt + seconds
  }
}

/**
  */
trait Transcoder[A] {
  def encode(value: A): ByteString
  def decode(str: ByteString): A

  def apply[B](e: B => A, d: A => B) = Transcoder[B](v => encode(e(v)), s => d(decode(s)))

  private[rcouch] def withEncoded[B](v: A)(f: ByteString => Future[B])(implicit ec: ExecutionContext) = f(encode(v))
}
trait AsyncTranscoder[A] extends Transcoder[A] {
  override private[rcouch] def withEncoded[B](v: A)(f: ByteString => Future[B])(implicit ec: ExecutionContext) = Future(encode(v)) flatMap f
}


object Transcoder {

  def apply[A](e: A => ByteString, d: ByteString => A): Transcoder[A] = new Transcoder[A] {
    def encode(value: A): ByteString = e(value)
    def decode(str: ByteString): A = d(str)
  }

  implicit val StringTranscoder = Transcoder[String](ByteString(_, "utf8"), _.decodeString("utf8"))
  implicit val IntTranscoder = StringTranscoder[Int](_.toString, _.toInt)
  implicit val LongTranscoder = StringTranscoder[Long](_.toString, _.toLong)
  implicit val FloatTranscoder = StringTranscoder[Float](_.toString, _.toFloat)
  implicit val DoubleTranscoder = StringTranscoder[Double](_.toString, _.toDouble)
  implicit val BooleanTranscoder = StringTranscoder[Boolean](_.toString, _.toBoolean)
  implicit val JsValueTranscoder = StringTranscoder[JsValue](_.compactPrint, JsonParser(_))
}
