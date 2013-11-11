package com.geteit.rcouch.views

import org.scalatest.{FeatureSpecLike, Matchers}
import akka.io.PipelineFactory
import akka.actor.ActorSystem
import spray.http.HttpData
import com.geteit.rcouch.views.ViewResponse.{HasActorSystem, PipelineStage}
import akka.testkit.TestKit
import com.geteit.rcouch.views.Query.StringKey
import spray.json.JsNull

/**
  */
class ViewResponseTest(_system: ActorSystem) extends TestKit(_system) with FeatureSpecLike with Matchers {

  def this() = this(ActorSystem("ViewResponseTest"))

  val viewResponse = """{"total_rows":5,"rows":[
                       |{"id":"57ce515e-0b74-4a88-8596-d0db728c46f5","key":"test1@gmail.com","value":null},
                       |{"id":"2672a23c-31a2-4afb-bb81-2ec31c2c6270","key":"test2@gmail.com","value":null},
                       |{"id":"30909729-9728-4f9b-9589-dbcd4f5a7a82","key":"test3@gmail.com","value":null},
                       |{"id":"ac08e648-f5bf-4537-935e-3e620276103b","key":"test4@mail.com","value":null},
                       |{"id":"d81522fe-db69-43fa-aa99-238a8d8779d3","key":"some_mail@gmail.com","value":null}
                       |]
                       |}""".stripMargin

  feature("PipelineStage") {

    scenario("Parse with regexps") {
      PipelineStage.TotalRows.unapplySeq(viewResponse) should be(Some(List("5", viewResponse.substring(16))))
      PipelineStage.RowsStart.unapplySeq(viewResponse.substring(16)) should be(Some(List(viewResponse.substring(25))))
      PipelineStage.RowsEnd.unapplySeq(" \n  ]\n}  ") should be(Some(List()))
    }

    scenario("Parse View Response") {
      val pp = PipelineFactory.buildFunctionTriple(
        new HasActorSystem() { val system = ViewResponseTest.this.system },
        new ViewResponse.PipelineStage)

      val events = pp.events(HttpData(viewResponse))._1.toList
      events.length should be(7)
      events(0) should be(ViewResponse.Start(5))
      events(1) should be(ViewResponse.Row(StringKey("test1@gmail.com"), JsNull, Some("57ce515e-0b74-4a88-8596-d0db728c46f5")))
      events(2) should be(ViewResponse.Row(StringKey("test2@gmail.com"), JsNull, Some("2672a23c-31a2-4afb-bb81-2ec31c2c6270")))
      events(6) should be(ViewResponse.End())
    }
  }
}
