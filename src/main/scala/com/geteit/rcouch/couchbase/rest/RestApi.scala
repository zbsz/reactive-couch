package com.geteit.rcouch.couchbase.rest

import spray.httpx.marshalling.Marshaller
import spray.http.{HttpResponse, Uri, ContentTypes, FormData}
import spray.http.MediaTypes._
import spray.http.ContentType._
import scala.util.Try

/**
  */
object RestApi {

  sealed trait AuthType { val str: String }
  object AuthType {
    object Sasl extends AuthType { val str = "sasl" }
    object None extends AuthType { val str = "none" }
  }

  sealed trait BucketType { val str: String }
  object BucketType {
    object Memcached extends BucketType { val str = "memcached" }
    object Couchbase extends BucketType { val str = "couchbase" }
  }

  case class RamQuota(megaBytes: Int) { require(megaBytes >= 100, "RamQuota should be at least 100MB") }
  case class ThreadsNumber(number: Int) { require(number >=2 && number <= 8, "ThreadsNumber should be in range (2 to 8)") }
  case class SaslPassword(passwd: String)

  sealed trait ReplicaNumber { val number: Int }
  object ReplicaNumber {
    object Zero extends ReplicaNumber { val number = 0 }
    object One extends ReplicaNumber { val number = 1 }
    object Two extends ReplicaNumber { val number = 2 }
    object Three extends ReplicaNumber { val number = 3 }
  }

  sealed trait RestResponse
  case class RestFailed(uri: Uri, res: Try[HttpResponse]) extends RestResponse
  case class BucketCreated(name: String) extends RestResponse
  case class BucketDeleted(name: String) extends RestResponse


  sealed trait RestCommand
  case class GetDesignDocs(bucket: String) extends RestCommand
  case class DeleteBucket(name: String) extends RestCommand
  case class CreateBucket(name: String,
                          ramQuota: RamQuota,
                          saslPassword: Option[String] = None,
                          bucketType: BucketType = BucketType.Couchbase,
                          authType: AuthType = AuthType.Sasl,
                          flushAllEnabled: Boolean = false,
                          parallelDBAndViewCompaction: Boolean = false,
                          proxyPort: Option[Int] = None,
                          replicaIndex: Boolean = true,
                          replicaNumber: ReplicaNumber = ReplicaNumber.One,
                          threads: Option[ThreadsNumber] = None
                           ) extends RestCommand {

    // TODO: we should encode this logic in type system
    if (authType == AuthType.Sasl) require(saslPassword != None, "Password has to be specified when AuthType = Sasl")
    else require(proxyPort.isDefined, "Valid proxy port has to be specified when not using Sasl auth type")
  }

  object CreateBucket {

    implicit def marshaller = Marshaller.delegate[CreateBucket, FormData](`application/x-www-form-urlencoded`) { cmd: CreateBucket =>
      new FormData((Seq(
          ("name", cmd.name),
          ("authType", cmd.authType.str),
          ("ramQuotaMB", cmd.ramQuota.megaBytes.toString),
          ("bucketType", cmd.bucketType.str),
          ("flushEnabled", if (cmd.flushAllEnabled) "1" else "0"),
          ("replicaIndex", if (cmd.replicaIndex) "1" else "0"),
          ("replicaNumber", cmd.replicaNumber.number.toString),
          ("parallelDBAndViewCompaction", cmd.parallelDBAndViewCompaction.toString)
        ) ++ Seq(
          ("saslPassword", cmd.saslPassword),
          ("proxyPort", cmd.proxyPort),
          ("proxyPort", cmd.proxyPort)
        ).filter(_._2.isDefined).map(p => (p._1, p._2.get.toString))).toSeq
      )
    }
  }

}
