package com.geteit.rcouch.couchbase

import spray.json.{JsonReader, JsonParser, DefaultJsonProtocol}
import akka.event.Logging
import spray.http._
import akka.io.{SymmetricPipePair, HasActorContext, SymmetricPipelineStage}
import java.io.{File, FileWriter}

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

  trait JsonProtocol extends DefaultJsonProtocol {
    implicit val PortsFormat = jsonFormat2(Ports)
    implicit val NodeFormat = jsonFormat4(Node)
    implicit val VBucketMapFormat = jsonFormat4(VBucketMap)
    implicit val BucketFormat = jsonFormat6(Bucket)
  }

  class CouchbaseException(msg: String, cause: Throwable = null) extends Exception(msg, cause)
}

class ChunkedParserPipelineStage[A <: AnyRef : JsonReader] extends SymmetricPipelineStage[HasActorContext, A, HttpMessagePart] {
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
        implicitly[JsonReader[A]].read(JsonParser(json))
      } catch {
        case e: Exception =>
          log.error(e, s"error while parsing chunk: $json")
          throw e
      }
    }
  }
}
