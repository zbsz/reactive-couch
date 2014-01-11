package com.geteit.rcouch.views

import java.net.URLEncoder
import Query._
import spray.http.Uri
import play.api.libs.json._
import play.api.libs.json.JsString
import scala.Some
import com.geteit.rcouch.views.Query.BBox
import scala.reflect.macros.Context

/**
 * The Query class allows custom view-queries to the Couchbase cluster.
 *
 * The Query class supports all arguments that can be passed along with a
 * Couchbase view query. For example, this makes it possible to change the
 * sorting order, query only a range of keys or include the full docs.
 *
 * By default, the full docs are not included and no reduce job is executed.
 */
case class Query(key: Option[Key] = None,
                 keys: List[Key] = Nil,
                 group: Option[Boolean] = None,
                 groupLevel: Option[Int] = None,
                 limit: Option[Int] = None,
                 skip: Option[Int] = None,
                 startKey: Option[Key] = None,
                 endKey: Option[Key] = None,
                 inclusiveEnd: Option[Boolean] = None,
                 startKeyDocId: Option[String] = None,
                 endKeyDocId: Option[String] = None,
                 onError: Option[OnError] = None,
                 debug: Option[Boolean] = None,
                 bBox: Option[BBox] = None,
                 descending: Option[Boolean] = None,
                 stale: Option[Stale] = None,
                 reduce: Option[Boolean] = None,
                 includeDocs: Boolean = false
            ) {

  /**
   * Returns the Query object as a string, suitable for the HTTP queries.
   *
   * @return Returns the query object as its string representation
   */
  override def toString: String = httpQuery.toString()

  def httpQuery: Uri.Query = {
    Query.buildQuery(
      ("key", key),
      ("keys", if (keys.isEmpty) None else Some(keys.mkString("[", ",", "]"))),
      ("group", group),
      ("group_level", groupLevel),
      ("limit", limit),
      ("skip", skip),
      ("startkey", startKey),
      ("endkey", endKey),
      ("startkey_docid", startKeyDocId),
      ("endkey_docid", endKeyDocId),
      ("inclusive_end=", inclusiveEnd),
      ("reduce", reduce),
      ("on_error", onError),
      ("bbox", bBox),
      ("debug", debug),
      ("stale", stale),
      ("descending", descending)
    )
  }
}

object Query {

  sealed trait StrArg {
    protected val str: String
    override def toString: String = str
  }

  sealed abstract class OnError(protected val str: String) extends StrArg
  object OnError {
    case object Stop extends OnError("stop")
    case object Continue extends OnError("continue")
  }

  sealed abstract class Stale(protected val str: String) extends StrArg
  object Stale {
    case object Ok extends Stale("ok")
    case object False extends Stale("false")
    case object UpdateAdter extends Stale("update_after")
  }

  case class BBox(lowerLeftLong: Double, lowerLeftLat: Double, upperRightLong: Double, upperRightLat: Double) {
    override def toString = lowerLeftLong + "," + lowerLeftLat + "," + upperRightLong + "," + upperRightLat
  }

  private def buildQuery(parts: (String, Option[Any])*): Uri.Query = parts.foldLeft(Uri.Query.Empty: Uri.Query)((q, p) => p match {
    case (k, Some(v)) => (k, v.toString) +: q
    case _ => q
  })
}

case class Key(json: JsValue) {
  override def toString: String = Json.stringify(json)
}

object Key {
  import scala.language.implicitConversions

  object String {
    def unapply(key: Key) = key match {
      case Key(JsString(str)) => Some(str)
      case _ => None
    }
  }

  object Boolean {
    def unapply(key: Key) = key match {
      case Key(JsBoolean(v)) => Some(v)
      case _ => None
    }
  }

  implicit val fmt = new Format[Key]{
    def reads(json: JsValue): JsResult[Key] = JsSuccess(Key(json))
    def writes(o: Key): JsValue = o.json
  }
  implicit def value_to_key[A](v: A)(implicit writes: Writes[A]) = Key(writes.writes(v))

  /*
   * Creates key from tuple.
   *
   * TODO: reimplement - could probably be better handled as macro, current implementation is not typesafe and very limited
   */
  implicit def product_to_key[A <: Product](v: A) = Key(JsArray(
    v.productIterator.map {
      case s: String => JsString(s)
      case c: Char => JsString(c.toString)
      case b: Boolean => JsBoolean(b)
      case i: Int => JsNumber(i)
      case d: Double => JsNumber(d)
      case f: Float => JsNumber(f)
      case _: Unit => Json.obj()
      case elem => throw new IllegalArgumentException(s"Unexpected tuple key item: $elem")
    }.toSeq
  ))
}
