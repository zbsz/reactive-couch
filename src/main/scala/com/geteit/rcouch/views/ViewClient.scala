package com.geteit.rcouch.views

import com.geteit.rcouch.Client
import play.api.libs.iteratee.Enumerator
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

/**
  */
trait ViewClient extends Client {
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

  def query[A](v: View, q: Query)(implicit timeout: Timeout = 15.seconds): Enumerator[ViewResponse.Row[A]] = new QueryExecutor(v, q).apply[A](this)
}