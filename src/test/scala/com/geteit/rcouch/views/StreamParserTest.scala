package com.geteit.rcouch.views

import org.scalatest.{Matchers, FeatureSpec}
import com.google.gson.stream.JsonReader
import java.io._
import com.google.gson.{JsonParser, Gson}
import spray.json.{JsObject, JsonParser}
import com.google.gson.JsonParser
import org.parboiled.scala._
import org.parboiled.errors.{ParseError, ErrorUtils, ParsingException}
import org.parboiled.{MatchHandler, MatcherContext}
import org.parboiled.buffers.DefaultInputBuffer
import org.parboiled.support.DefaultValueStack
import java.util
import org.parboiled.matchers.Matcher

/**
  */
class StreamParserTest extends FeatureSpec with Matchers {

  feature("StreamParser") {
    scenario("Read ViewResponse start") {
      val viewResponseStart = """{"total_rows":600,"rows":[
                                |{"id":"57ce515e-0b74-4a88-8596-d0db728c46f5","key":"12kiss.gabor34@gmail.com","value":null},
                                |{"id":"2672a23c-31a2-4afb-bb81-2ec31c2c6270","key":"250380pvn.97@gmail.com","value":null},
                                |{"id":"30909729-9728-4f9b-9589-dbcd4f5a7a82","key":"2fly2saucy@gmail.com","value":null},
                                |{"id":"ac08e648-f5bf-4537-935e-3e620276103b","key":"777macorchards@gmail.com","value":null},
                                |{"id":"d81522fe-db69-43fa-aa99-238a8d8779d3","key":"93fomoco@gmail.com","value":null},""".stripMargin
      val viewResponse2Docs = """
                                |{"id":"2df5f34b-5d6d-4d60-8d84-468bde27dfc3","key":"666masterdevil666@gmail.com","value":null},
                                |{"id":"859ce63f-140b-43e7-82c4-56a0b55651a2","key":"666masterdevil666@gmail.com","value":null},""".stripMargin

      val out = new PipedOutputStream()

      class NotAvailableException extends RuntimeException

      val nonBlockingReader = new Reader() {

        val in = new InputStreamReader(new PipedInputStream(out))

        def close(): Unit = in.close()

        def read(buffer: Array[Char], offset: Int, limit: Int): Int = {
          if (in.ready()) in.read(buffer, offset, limit)
          else throw new NotAvailableException()
        }
      }


      val reader = new JsonReader(nonBlockingReader)
      val parser = new JsonParser()

      out.write(viewResponseStart.getBytes)

      reader.beginObject()
      reader.nextName() should be("total_rows")
      reader.nextInt() should be(600)
      reader.nextName() should be("rows")
      reader.beginArray()
      for (i <- 1 to 5) {
        parser.parse(reader)
      }
      intercept[NotAvailableException] {
        reader.peek()
      }
//      out.write(viewResponse2Docs.getBytes)
//      for (i <- 1 to 2) {
//        parser.parse(reader)
//      }
//      intercept[NotAvailableException] {
//        reader.peek()
//      }

      out.close()
    }

    scenario("Parse View rows") {
      val viewResponse2Docs = """{"id":"2df5f34b-5d6d-4d60-8d84-468bde27dfc3","key":"666masterdevil666@gmail.com","value":null},
                                {"id":"859ce63f-140b-43e7-82c4-56a0b55651a2","key":"666masterdevil666@gmail.com","value":null},"""

      val JsonObjectRule = spray.json.JsonParser.JsonObject

      val valueStack = new DefaultValueStack[JsObject]
      val context = new MatcherContext(
        new DefaultInputBuffer(viewResponse2Docs.toCharArray),
        valueStack,
        new util.ArrayList[ParseError],
        new MatchHandler() {
          def `match`(context: MatcherContext[_]): Boolean = context.getMatcher.`match`(context)
        },
        JsonObjectRule.matcher,
        true)

      context.runMatcher() should be(true)

      info(s"Current index: ${context.getCurrentIndex}")
      info(s"Result: ${valueStack.peek()}")
    }
  }
}
