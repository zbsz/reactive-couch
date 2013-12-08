package com.geteit.rcouch

import org.scalatest._
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import com.geteit.rcouch.memcached._
import akka.util.Timeout
import org.scalatest.matchers.{MatchResult, Matcher}
import com.geteit.rcouch.Settings.ClusterSettings
import scala.Some
import scala.Some

/**
  */
class MemcachedClientSpec extends FeatureSpec with Matchers with BeforeAndAfterAll with FutureMatcher {

  val settings = ClusterSettings()
  var couchbase: CouchbaseClient = _
  var client: MemcachedClient = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    couchbase = new CouchbaseClient(settings)
    client = Await.result(couchbase.bucket("geteit"), 5.seconds)
  }

  override protected def afterAll(): Unit = {
    couchbase.close()
    super.afterAll()
  }

  feature("Memcached operations") {
    scenario("Set string value") {
      client.set("key", "test_value", Expire.After(30)) should evalTo(true)
    }

    scenario("Get nonexistent value") {
      client.get[String]("non_existent_key_1234#!@43") should evalTo(None)
    }

    scenario("Get string value") {
      client.set("key", "test_value", Expire.After(30)) should evalTo(true)
      client.get[String]("key") should evalTo(Some("test_value"))
    }

    scenario("Get value with cas") {
      client.set("key", "test_value", Expire.After(30)) should evalTo(true)
      eval(client.gets[String]("key")).map(_.v) should be(Some("test_value"))
    }

    scenario("Cas unchanged value") {
      client.set("key", "test_value", Expire.After(30)) should evalTo(true)
      val cas = eval(client.gets[String]("key")).get.cas
      client.cas("key", "new_value", cas) should evalTo(CasResponse.Ok)
      client.get[String]("key") should evalTo(Some("new_value"))
    }

    scenario("Cas non existent value") {
      client.cas("non_existent_key_1!@#4", "new_value", CasId(1)) should evalTo(CasResponse.NotFound)
      client.get[String]("non_existent_key_1!@#4") should evalTo(None)
    }

    scenario("Cas changed value") {
      client.set("key", "test_value", Expire.After(30)) should evalTo(true)
      val cas = eval(client.gets[String]("key")).get.cas
      client.set("key", "test_value1", Expire.After(30)) should evalTo(true)
      client.cas("key", "new_value", cas) should evalTo(CasResponse.Exists)
      client.get[String]("key") should evalTo(Some("test_value1"))
    }

    scenario("Replace string value") {
      client.set("key", "test_value", Expire.After(30)) should evalTo(true)
      client.replace("key", "new_test_value", Expire.After(30)) should evalTo(true)
      client.get[String]("key") should evalTo(Some("new_test_value"))
    }

    scenario("Delete value") {
      client.set("key", "test_value", Expire.After(30)) should evalTo(true)
      client.delete("key") should evalTo(true)
      client.get[String]("key") should evalTo(None)
      client.delete("key") should evalTo(false)
    }
  }

}

trait FutureMatcher {
  self: Matchers =>

  def eval[A](f: Future[A])(implicit t: Timeout = 5.seconds) = Await.result(f, t.duration)
  
  def evalTo(right: Any)(implicit t: Timeout = 5.seconds): Matcher[Future[Any]] = {
    new Matcher[Future[Any]] {
      val internal = be(right)
      def apply(left: Future[Any]): MatchResult = internal.apply(eval(left))
    }
  }
}
