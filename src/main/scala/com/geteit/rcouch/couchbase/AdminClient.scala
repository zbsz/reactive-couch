package com.geteit.rcouch.couchbase

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import com.geteit.rcouch.Client
import com.geteit.rcouch.couchbase.rest.RestApi._
import com.geteit.rcouch.couchbase.rest.RestApi.RamQuota
import akka.pattern.ask
import com.geteit.rcouch.couchbase.Couchbase.CouchbaseException
import spray.http.HttpCharsets

/**
 * Handles administrative REST api.
 */
trait AdminClient extends Client {

  def createCouchbaseBucket(name: String,
                            saslPassword: String,
                            ramQuota: RamQuota = RamQuota(100),
                            flushAllEnabled: Boolean = false,
                            parallelDBAndViewCompaction: Boolean = false,
                            replicaIndex: Boolean = true,
                            replicaNumber: ReplicaNumber = ReplicaNumber.One,
                            threads: Option[ThreadsNumber] = None): Future[Unit] = {

    val command = CreateBucket(name, ramQuota, Option(saslPassword), BucketType.Couchbase, AuthType.Sasl,
                               flushAllEnabled, parallelDBAndViewCompaction, None, replicaIndex, replicaNumber, threads)

    actor ? command map {
      case BucketCreated(_) => Unit
      case RestFailed(_, Success(r)) => throw new CouchbaseException("Create bucket failed: " + r.entity.asString(HttpCharsets.`UTF-8`))
      case RestFailed(_, Failure(e)) => throw new CouchbaseException("Create bucket failed.", e)
    }
  }

  def deleteBucket(name: String): Future[Unit] = {
    actor ? DeleteBucket(name) map {
      case BucketDeleted(_) => Unit
      case RestFailed(_, Success(r)) => throw new CouchbaseException("Delete bucket failed: " + r.entity.asString(HttpCharsets.`UTF-8`))
      case RestFailed(_, Failure(e)) => throw new CouchbaseException("Delete bucket failed.", e)
    }
  }
}
