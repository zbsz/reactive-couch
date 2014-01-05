package com.geteit.rcouch.views

import akka.io._
import spray.http.{HttpCharsets, HttpData}
import com.geteit.rcouch.views.Query.Key
import akka.event.Logging
import akka.actor.ActorSystem
import play.api.libs.json._
import scala.Some
import play.api.libs.json.JsObject
import org.parboiled.support.DefaultValueStack
import org.parboiled.errors.ParseError
import org.parboiled.{MatcherContext, MatchHandler}
import org.parboiled.buffers.DefaultInputBuffer
import org.parboiled.scala.Parser
import java.util

/**
  */
sealed trait ViewResponse

object ViewResponse {

  case class Start(rowsCount: Int = 0) extends ViewResponse

  case class Row(key: Key, value: JsValue, id: Option[String]) extends ViewResponse

  object Row {
    implicit val fmt = Json.format[Row]
  }

  case class End() extends ViewResponse

  case class Error(message: String) extends Exception(message) with ViewResponse

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
          val Row = rule { optional(str(",")) ~ ParboiledJsonParser.WhiteSpace ~ ParboiledJsonParser.JsonObject }
        }

        val valueStack = new DefaultValueStack[JsObject]
        val errors = new util.ArrayList[ParseError]

        val matchHandler = new MatchHandler {
          override def `match`(context: MatcherContext[_]): Boolean = context.getMatcher.`match`(context)
        }

        def unapply(input: String): Option[(Row, String)] = {
          valueStack.clear()
          errors.clear()

          val context = new MatcherContext(new DefaultInputBuffer(input.toCharArray), valueStack, errors, matchHandler, ViewRowParser.Row.matcher, true)
          if (context.runMatcher()) {
            Some(valueStack.pop().as[Row], input.drop(context.getCurrentIndex))
          } else {
            log.debug(s"Streamed view row parsing failed (as expected when streaming): $errors for input: $input")
            None
          }
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
