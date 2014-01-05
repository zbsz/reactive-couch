package com.geteit.rcouch.views

import org.scalatest.{FeatureSpecLike, Matchers}
import akka.io.PipelineFactory
import akka.actor.ActorSystem
import spray.http.HttpData
import com.geteit.rcouch.views.ViewResponse.{HasActorSystem, PipelineStage}
import akka.testkit.TestKit
import play.api.libs.json.JsNull
import org.scalacheck.Gen

import org.scalatest.prop.Checkers
import org.scalacheck.Prop._

/**
  */
class ViewResponseTest(_system: ActorSystem) extends TestKit(_system) with FeatureSpecLike with Matchers with Checkers {

  def this() = this(ActorSystem("ViewResponseTest"))

  val viewResponse = """{"total_rows":5,"rows":[
                       |{"id":"57ce515e-0b74-4a88-8596-d0db728c46f5","key":"test1@gmail.com","value":null},
                       |{"id":"2672a23c-31a2-4afb-bb81-2ec31c2c6270","key":"test2@gmail.com","value":null},
                       |{"id":"30909729-9728-4f9b-9589-dbcd4f5a7a82","key":"test3@gmail.com","value":null},
                       |{"id":"ac08e648-f5bf-4537-935e-3e620276103b","key":"test4@mail.com","value":null},
                       |{"id":"d81522fe-db69-43fa-aa99-238a8d8779d3","key":"some_mail@gmail.com","value":null}
                       |]
                       |}""".stripMargin

  def substrings(str: String): Gen[List[String]] =
    if (str.length < 2) Gen.value(List(str))
    else Gen.choose(1, str.length) flatMap { i =>
      substrings(str.substring(i)) map { ls => str.substring(0, i) :: ls }
    }

  feature("PipelineStage") {

    scenario("Parse View Response") {
      val pp = PipelineFactory.buildFunctionTriple(
        new HasActorSystem() { val system = ViewResponseTest.this.system },
        new ViewResponse.PipelineStage)

      val events = pp.events(HttpData(viewResponse))._1.toList
      events.length should be(7)
      events(0) should be(ViewResponse.Start(5))
      events(1) should be(ViewResponse.Row("test1@gmail.com", JsNull, Some("57ce515e-0b74-4a88-8596-d0db728c46f5")))
      events(2) should be(ViewResponse.Row("test2@gmail.com", JsNull, Some("2672a23c-31a2-4afb-bb81-2ec31c2c6270")))
      events(6) should be(ViewResponse.End())
    }

    scenario("Parse View Response in fragments") {
      check(forAll(substrings(viewResponse)) { ls =>
        ls.fold("")(_ + _) == viewResponse ==> {

          val pp = PipelineFactory.buildFunctionTriple(
            new HasActorSystem() { val system = ViewResponseTest.this.system },
            new ViewResponse.PipelineStage)

          val events = ls.map(str => pp.events(HttpData(str))._1.toList).flatten

          events == List(
            ViewResponse.Start(5),
            ViewResponse.Row("test1@gmail.com", JsNull, Some("57ce515e-0b74-4a88-8596-d0db728c46f5")),
            ViewResponse.Row("test2@gmail.com", JsNull, Some("2672a23c-31a2-4afb-bb81-2ec31c2c6270")),
            ViewResponse.Row("test3@gmail.com", JsNull, Some("30909729-9728-4f9b-9589-dbcd4f5a7a82")),
            ViewResponse.Row("test4@mail.com", JsNull, Some("ac08e648-f5bf-4537-935e-3e620276103b")),
            ViewResponse.Row("some_mail@gmail.com", JsNull, Some("d81522fe-db69-43fa-aa99-238a8d8779d3")),
            ViewResponse.End())
        }
      })
    }
  }
}
