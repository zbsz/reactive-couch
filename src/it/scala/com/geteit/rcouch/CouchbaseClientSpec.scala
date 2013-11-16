package com.geteit.rcouch

import org.scalatest.{BeforeAndAfter, FeatureSpec, Matchers}
import com.geteit.rcouch.views.{ViewResponse, Query, View}
import com.geteit.rcouch.Settings.ClusterSettings
import play.api.libs.iteratee.Iteratee
import com.geteit.rcouch.views.ViewResponse.Row
import scala.concurrent.{ExecutionContext, Await}
import concurrent.duration._
import ExecutionContext.Implicits.global

/**
  */
class CouchbaseClientSpec extends FeatureSpec with Matchers with BeforeAndAfter {

  val settings = new ClusterSettings("geteit", List("http://localhost:8091/pools"))
  var client: CouchbaseClient = _
  
  before {
    client = new CouchbaseClient(settings)
  }
  
  after {
    client.close()
  }
  
  feature("CouchbaseClient") {
    scenario("Connect and send view query") {
      val enum = client.query[Any](View("user_by_email", "geteit", "users"), Query())
      val result = enum |>>> Iteratee.fold(Nil: List[Row[Any]])((l, row) => row :: l)

      info(s"Got result list of len: ${Await.result(result, 10.seconds).length}")
    }

    scenario("Try to query non existent view") {
      val enum = client.query[Any](View("non_existent_view", "geteit", "users"), Query())
      val result = enum |>>> Iteratee.fold(Nil: List[Row[Any]])((l, row) => row :: l)
      
      intercept[ViewResponse.Error] {
        info(s"Got result: ${Await.result(result, 10.seconds)}")
      }
    }

    scenario("Start and shutdown client immediately") {
      // do nothing
    }

    ignore("Run idle for 30 seconds") {
      Thread.sleep(30.seconds.toMillis)
    }
  }
}