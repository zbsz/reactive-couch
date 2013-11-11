package com.geteit.rcouch.views

import java.net.URLEncoder
import Query._
import spray.http.Uri

/**
 * The Query class allows custom view-queries to the Couchbase cluster.
 *
 * The Query class supports all arguments that can be passed along with a
 * Couchbase view query. For example, this makes it possible to change the
 * sorting order, query only a range of keys or include the full docs.
 *
 * By default, the full docs are not included and no reduce job is executed.
 */
case class Query(key: Option[Key[_]] = None,
                 keys: List[Key[_]] = Nil,
                 group: Option[Boolean] = None,
                 groupLevel: Option[Int] = None,
                 limit: Option[Int] = None,
                 skip: Option[Int] = None,
                 startKey: Option[Key[_]] = None,
                 endKey: Option[Key[_]] = None,
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
    def enc(v: Any) = URLEncoder.encode(v.toString, "UTF-8")

    Query.buildQuery(
      ("key", key map enc),
      ("keys", if (keys.isEmpty) None else Some(keys.map(enc).mkString("[", ",", "]"))),
      ("group", group),
      ("group_level", groupLevel),
      ("limit", limit),
      ("skip", skip),
      ("startkey", startKey map enc),
      ("endkey", endKey map enc),
      ("startkey_docid", startKeyDocId map enc),
      ("endkey_docid", endKeyDocId map enc),
      ("inclusive_end=", inclusiveEnd),
      ("reduce", reduce),
      ("on_error", onError),
      ("bbox", bBox map enc),
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

  sealed trait Key[A] {
    val value: A
    val json: String
    override def toString: String = json
  }

  case class StringKey(value: String) extends Key[String] {
    override val json = "\"" + value + "\""
  }
  case class BooleanKey(value: Boolean) extends Key[Boolean] {
    override val json = value.toString
  }
  case class NumKey[A: Numeric](value: A) extends Key[A] {
    override val json = value.toString
  }
  case class ArrayKey[A](value: Array[A])(implicit conv: A => Key[A]) extends Key[Array[A]] {
    override val json = value.map(conv(_).json).mkString("[", ",", "]")
  }

  object Key {
    import scala.language.implicitConversions

    implicit def string_to_key(str: String) = StringKey(str)
    implicit def boolean_to_key(b: Boolean) = BooleanKey(b)
    implicit def number_to_key[A: Numeric](n: A) = NumKey(n)
    implicit def pair_to_key[A, B](p: (A, B))(implicit aconv: A => Key[A], bconv: B => Key[B]) = new Key[(A, B)] {
      override val value = p
      override val json = s"[${aconv(p._1).json},${bconv(p._2).json}}]"
    }
  }

  case class BBox(lowerLeftLong: Double, lowerLeftLat: Double, upperRightLong: Double, upperRightLat: Double) {
    override def toString = lowerLeftLong + "," + lowerLeftLat + "," + upperRightLong + "," + upperRightLat
  }

  private def buildQuery(parts: (String, Option[Any])*): Uri.Query = parts.foldLeft(Uri.Query.Empty: Uri.Query)((q, p) => p match {
    case (k, Some(v)) => (k, v.toString) +: q
    case _ => q
  })
}
