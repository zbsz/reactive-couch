package com.geteit.rcouch.views

import com.geteit.rcouch.Client
import play.api.libs.iteratee.{Iteratee, Enumeratee, Enumerator}
import scala.concurrent.Future
import com.geteit.rcouch.actors.ViewActor._
import akka.pattern.ask
import com.geteit.rcouch.couchbase.rest.RestApi
import com.geteit.rcouch.couchbase.Couchbase.CouchbaseException
import com.geteit.rcouch.actors.ViewActor.SaveDesignDoc
import com.geteit.rcouch.actors.ViewActor.DeleteDesignDoc
import com.geteit.rcouch.actors.ViewActor.GetDesignDoc
import scala.Some
import com.geteit.rcouch.views.DesignDocument.DocumentDef
import akka.util.Timeout
import concurrent.duration._
import com.geteit.rcouch.memcached.{MemcachedClient, Transcoder}
import scala.collection.mutable.ListBuffer
import com.geteit.rcouch.views.Query.Stale

/**
  */
trait ViewClient extends Client { self: MemcachedClient =>
  val bucket: String
  import RestApi._

  def getDesignDocument(name: String): Future[DesignDocument] = mapRestTo[DesignDocument](actor ? GetDesignDoc(name))

  def saveDesignDocument(name: String, doc: DocumentDef): Future[Unit] = mapRestTo[Saved.type](actor ? SaveDesignDoc(name, doc)) map(_ => {})

  def saveDesignDocument(doc: DesignDocument): Future[Unit] = mapRestTo[Saved.type](actor ? SaveDesignDoc(doc.name, DocumentDef(doc.views))) map(_ => {})

  def deleteDesignDocument(name: String): Future[Unit] = mapRestTo[Deleted.type](actor ? DeleteDesignDoc(name)) map(_ => {})

  def getView(view: String, designDocument: String): Future[View] =
    getDesignDocument(designDocument) flatMap (_.view(view) match {
      case Some(v) => Future.successful(v)
      case _ => Future.failed(new CouchbaseException(s"View not found: $view"))
    })

  def query(v: View, q: Query)(implicit timeout: Timeout = 15.seconds): Enumerator[ViewResponse.Row] = new QueryExecutor(v, q).apply(this)

  def enumerate[A](v: View, q: Query)(implicit timeout: Timeout = 15.seconds, evidence: Transcoder[A]): Enumerator[Option[A]] =
    query(v, q) &> Enumeratee.mapM[ViewResponse.Row](row => row.id.fold(Future.successful(None: Option[A]))(id => get[A](id)))

  def list[A](v: View, q: Query)(implicit timeout: Timeout = 15.seconds, evidence: Transcoder[A]): Future[List[Option[A]]] =
    enumerate[A](v, q) |>>> Iteratee.fold(new ListBuffer[Option[A]])(_ += _) map (_.toList)

  def forceRefresh(v: View)(implicit timeout: Timeout = 15.seconds): Future[Unit] =
    query(v, Query(stale = Some(Stale.False), limit = Some(1))) |>>> Iteratee.foreach((_: ViewResponse.Row) => ())
}
