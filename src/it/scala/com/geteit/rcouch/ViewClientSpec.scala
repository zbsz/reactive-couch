package com.geteit.rcouch

import org.scalatest.{Outcome, fixture, Matchers}
import com.geteit.rcouch.views._
import com.geteit.rcouch.Settings.ClusterSettings
import play.api.libs.iteratee.Iteratee
import scala.concurrent.{ExecutionContext, Await}
import concurrent.duration._
import ExecutionContext.Implicits.global
import com.geteit.rcouch.views.View
import com.geteit.rcouch.views.ViewResponse.Row
import com.geteit.rcouch.views.DesignDocument.{DocumentDef, MapFunction, ViewDef}
import com.geteit.rcouch.couchbase.Couchbase.Bucket

/**
  */
class ViewClientSpec extends fixture.FeatureSpec with Matchers with BucketSpec {

  val settings = ClusterSettings()
  var couchbase: CouchbaseClient = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    couchbase = new CouchbaseClient(settings)
  }

  override protected def afterAll(): Unit = {
    couchbase.close()
    super.afterAll()
  }

  val ddoc = DocumentDef(Map(
    "all_docs" -> ViewDef(MapFunction("function (doc, meta) { emit(meta.id, null); }"))
  ))

  withDocuments("ddoc" -> ddoc)

    // TODO: populate db with documents

  feature("ViewClient") {
    scenario("Try to query non existent view") { bucket: Bucket =>
      val client = Await.result(couchbase.bucket(bucket.name), 10.seconds)
      val doc = new DesignDocument("ddoc", bucket.name, Map())
      val enum = client.query[Any](View("non_existent_view", doc), Query())
      val result = enum |>>> Iteratee.fold(Nil: List[Row[Any]])((l, row) => row :: l)

      intercept[ViewResponse.Error] {
        info(s"Got result: ${Await.result(result, 10.seconds)}")
      }
    }

    scenario("Connect and send view query") { bucket: Bucket =>
      val client = Await.result(couchbase.bucket(bucket.name), 10.seconds)
      val doc = new DesignDocument("ddoc", bucket.name, Map())
      val enum = client.query[Any](View("all_docs", doc), Query())(30.seconds)
      val result = enum |>>> Iteratee.fold(Nil: List[Row[Any]])((l, row) => row :: l)

      info(s"Got result list of len: ${Await.result(result, 30.seconds).length}")
    }

    scenario("Start and shutdown client immediately") { _ =>
      val client = new CouchbaseClient(settings)
      client.close()
    }

    ignore("Run idle for 30 seconds") { _ =>
      Thread.sleep(30.seconds.toMillis)
    }
  }
}
