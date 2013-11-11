package com.geteit.rcouch.views

import akka.io._
import spray.http.{HttpCharsets, HttpData}
import java.io.{PipedInputStream, InputStreamReader, PipedOutputStream}
import com.google.gson.stream.JsonReader
import com.google.gson.{JsonStreamParser, JsonElement}
import com.geteit.rcouch.views.Query.{ArrayKey, BooleanKey, StringKey, Key}
import org.parboiled.scala._
import spray.json._
import org.parboiled.{MatchHandler, MatcherContext, Context}
import org.parboiled.errors.{ParseError, ErrorUtils, ParsingException}
import scala.util.parsing.combinator.JavaTokenParsers
import org.parboiled.support.DefaultValueStack
import org.parboiled.buffers.DefaultInputBuffer
import java.util
import akka.event.Logging
import akka.actor.ActorSystem

/**
  */
sealed trait ViewResponse {

}

object ViewResponse {

  case class Start(rowsCount: Int = 0) extends ViewResponse

  case class Row[A](key: Key[A], value: JsValue, id: Option[String]) extends ViewResponse

  case class End() extends ViewResponse

  case class Error(message: String) extends ViewResponse

  trait HasActorSystem extends PipelineContext {
    val system: ActorSystem
  }

  class PipelineStage extends SymmetricPipelineStage[HasActorSystem, ViewResponse, HttpData] {
    def apply(ctx: HasActorSystem) = new SymmetricPipePair[ViewResponse, HttpData] {

      import PipelineStage._

      val log = Logging(ctx.system, classOf[PipelineStage])
      var buffer = ""

      def extractResponses(input: String): List[ViewResponse] = input match {
        case TotalRows(rows, rest) =>
          ViewResponse.Start(rows.toInt) :: extractResponses(rest)
        case RowsStart(rest) =>
          extractResponses(rest)
        case RowsEnd() =>
          buffer = ""
          List(ViewResponse.End())
        case ViewRow(row, rest) =>
          row :: extractResponses(rest)
        case inputLeft =>
          buffer = inputLeft
          Nil
      }

      override def commandPipeline = { _ =>
        ???
      }

      override def eventPipeline = { resp =>
        buffer += resp.asString(HttpCharsets.`UTF-8`)
        extractResponses(buffer) match {
          case Nil => Nil
          case one :: Nil => ctx.singleEvent(one)
          case events => events.map(Left(_))
        }
      }

      object ViewRow {
        object ViewRowParser extends Parser {
          val Row = rule { optional(str(",")) ~ JsonParser.WhiteSpace ~ JsonParser.JsonObject }
        }

        val valueStack = new DefaultValueStack[JsObject]
        val errors = new util.ArrayList[ParseError]

        val matchHandler = new MatchHandler {
          override def `match`(context: MatcherContext[_]): Boolean = context.getMatcher.`match`(context)
        }

        def unapply(input: String): Option[(Row[_], String)] = {
          valueStack.clear()
          errors.clear()

          val context = new MatcherContext(new DefaultInputBuffer(input.toCharArray), valueStack, errors, matchHandler, ViewRowParser.Row.matcher, true)
          if (context.runMatcher()) {
            Some((row(valueStack.pop()), input.drop(context.getCurrentIndex)))
          } else {
            log.warning(s"View row parsing failed: $errors for input: $input")
            None
          }
        }

        def row(o: JsObject): Row[_] = Row(key(o.fields("key")), o.fields("value"), o.fields.get("id").map(_.asInstanceOf[JsString].value))

        def key(o: JsValue): Key[_] = o match {
          case JsString(v) => StringKey(v)
          case JsBoolean(b) => BooleanKey(b)
          case JsArray(elems) => ???
          case _ => StringKey(o.prettyPrint)
        }
      }
    }
  }

  object PipelineStage {
    val TotalRows = """(?s)\s*\{\s*"total_rows"\s*:\s*(\d+)\s*,(.*)""".r
    val RowsStart = """(?s)\s*"rows"\s*:\s*\[\s*(.*)""".r
    val RowsEnd = """\s*]\s*}\s*""".r
  }
}
