package com.geteit.rcouch.views

import org.scalatest.{Matchers, FeatureSpec}
import play.api.libs.json.{JsString, Json}

/**
  */
class QueryTest extends FeatureSpec with Matchers {

  scenario("Single key query") {
    val email = "test@gmail.com"
    val q = Query(Some(email))

    q.key should be(Some(Key(JsString(email))))
    q.httpQuery.toString() should equal("key=%22test@gmail.com%22")
  }

  scenario("Create key from tuple") {
    ((true, "str", 10): Key) should be(Key(Json.parse("[true,\"str\",10]")))
  }

  scenario("Query with tuple key") {
    val q = Query(Some((false, "test", 10)))

    q.key should be(Some(Key(Json.parse("[false,\"test\",10]"))))
    q.httpQuery.toString() should equal("key=%5Bfalse,%22test%22,10%5D")
  }

  scenario("Query with tuple key with Unit (for max possible value)") {
    val q = Query(Some((false, "test", ())))

    q.key should be(Some(Key(Json.parse("[false,\"test\",{}]"))))
    q.httpQuery.toString() should equal("key=%5Bfalse,%22test%22,%7B%7D%5D")
  }
}
