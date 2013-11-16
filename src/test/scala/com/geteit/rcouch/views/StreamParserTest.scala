package com.geteit.rcouch.views

import org.scalatest.{Matchers, FeatureSpec}
import spray.json.JsObject
import org.parboiled.errors.ParseError
import org.parboiled.{MatchHandler, MatcherContext}
import org.parboiled.buffers.DefaultInputBuffer
import org.parboiled.support.DefaultValueStack
import java.util

/**
  */
class StreamParserTest extends FeatureSpec with Matchers {

  feature("StreamParser") {
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
