package com.geteit.rcouch.actors

import org.scalatest.{Matchers, FeatureSpec}
import scala.io.Source
import spray.http.Uri
import play.api.libs.json.Json

/**
  */
class AdminActorTest extends FeatureSpec with Matchers {

  feature("REST parsing") {
    scenario("Parse Pool Response") {

      val str = Source.fromURL(classOf[AdminActorTest].getResource("/poolResponse.json")).mkString

      import AdminActor._
      val res = Json.parse(str).as[PoolRes]
    }
  }
  feature("Uri utils") {
    scenario("resolve uri") {
      val uri = Uri("http://localhost:8091")
      AdminActor.resolveUri(uri, "/pools") should be(Uri("http://localhost:8091/pools"))
    }
    scenario("resolve uri with path") {
      val uri = Uri("http://localhost:8091/pools/default")
      AdminActor.resolveUri(uri, "/pools") should be(Uri("http://localhost:8091/pools"))
    }
  }
}
