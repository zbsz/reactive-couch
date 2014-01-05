package com.geteit.rcouch.couchbase

import akka.event.Logging
import spray.http._
import akka.io.{SymmetricPipePair, HasActorContext, SymmetricPipelineStage}
import play.api.libs.json.{Reads, Json}

/**
  */
object Couchbase {

  case class Ports(proxy: Int, direct: Int)
  case class Node(couchApiBase: Option[String], hostname: String, status: String, ports: Ports) {
    def healthy = status == "healthy"
    def warmup = status == "warmup"
  }
  case class VBucketMap(hashAlgorithm: String, numReplicas: Int, serverList: Array[String], vBucketMap: Array[Array[Int]])
  case class Bucket(name: String, uri: String, streamingUri: String, saslPasswd: Option[String], nodes: Array[Node], vBucketServerMap: VBucketMap)

  implicit val PortsFormat = Json.format[Ports]
  implicit val NodeFormat = Json.format[Node]
  implicit val VBucketMapFormat = Json.format[VBucketMap]
  implicit val BucketFormat = Json.format[Bucket]

  class CouchbaseException(msg: String, cause: Throwable = null) extends Exception(msg, cause)
}

class ChunkedParserPipelineStage[A <: AnyRef : Reads] extends SymmetricPipelineStage[HasActorContext, A, HttpMessagePart] {
  def apply(ctx: HasActorContext) = new SymmetricPipePair[A, HttpMessagePart] {
    val log = Logging(ctx.getContext.system, classOf[ChunkedParserPipelineStage[A]])
    var buffer = ""

    override def commandPipeline = { _ => ??? } // we only support reading from chunked messages, writing is not supported

    override def eventPipeline = {
      case MessageChunk(body, _) =>
        log.debug("chunk: " + body)
        val str = body.asString(HttpCharsets.`UTF-8`)
        if (body.length == 0 || str == "\n\n\n\n") {
          val json = buffer
          buffer = ""
          ctx.singleEvent(parse(json))
        } else {
          buffer += str
          Nil
        }
      case _ => Nil
    }

    private def parse(json: String): A = {
      try {
        Json.parse(json).as[A]
      } catch {
        case e: Exception =>
          log.error(e, s"error while parsing chunk: $json")
          throw e
      }
    }
  }
}
