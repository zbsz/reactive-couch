package com.geteit.rcouch

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FeatureSpec, Matchers}
import com.geteit.rcouch.views._
import com.geteit.rcouch.Settings.ClusterSettings
import play.api.libs.iteratee.Iteratee
import com.geteit.rcouch.views.ViewResponse.Row
import scala.concurrent.{ExecutionContext, Await}
import concurrent.duration._
import ExecutionContext.Implicits.global
import com.geteit.rcouch.memcached.MemcachedClient
import com.geteit.rcouch.views.View
import com.geteit.rcouch.views.ViewResponse.Row

/**
  */
class ViewClientSpec extends FeatureSpec with Matchers with BeforeAndAfterAll {

  val settings = ClusterSettings()
  var couchbase: CouchbaseClient = _
  var client: ViewClient = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    couchbase = new CouchbaseClient(settings)
    client = Await.result(couchbase.bucket("geteit"), 5.seconds)
  }

  override protected def afterAll(): Unit = {
    couchbase.close()
    super.afterAll()
  }
  
  feature("CouchbaseClient") {
    scenario("Connect and send view query") {
      val doc = new DesignDocument("users", "geteit", Map())
      val enum = client.query[Any](View("user_by_email", doc), Query())
      val result = enum |>>> Iteratee.fold(Nil: List[Row[Any]])((l, row) => row :: l)

      info(s"Got result list of len: ${Await.result(result, 10.seconds).length}")
    }

    scenario("Try to query non existent view") {
      val doc = new DesignDocument("users", "geteit", Map())
      val enum = client.query[Any](View("non_existent_view", doc), Query())
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
