package com.geteit.rcouch

import org.scalatest.{Outcome, fixture, BeforeAndAfterAll}
import com.geteit.rcouch.couchbase.rest.RestApi.{ReplicaNumber, RamQuota}
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import com.geteit.rcouch.couchbase.Couchbase.{CouchbaseException, Bucket}
import com.geteit.rcouch.actors.AdminActor.GetBucket
import akka.pattern._
import akka.util.Timeout
import scala.util.Random
import com.geteit.rcouch.views.DesignDocument.DocumentDef

/**
  */
trait CouchSpec extends BeforeAndAfterAll { this: fixture.Suite  =>

  private var client: CouchbaseClient = _
  private val bucketName = s"bucket_$uniqueId"
  private var bucket: Bucket = _
  private implicit val timeout = 10.seconds : Timeout

  private var initFunc = List[BucketClient => Unit]()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    client = new CouchbaseClient()

    try {
      implicit val ec = client.system.dispatcher
      bucket = Await.result(
        for {
          _ <-  client.createCouchbaseBucket(bucketName, "", RamQuota(100), replicaNumber = ReplicaNumber.Zero)
          bucket <- waitForBucket(bucketName)
          bc <- client.bucket(bucketName)
        } yield { initBucket(bc); bucket },
        25.seconds
      )
    } catch {
      case e: Exception =>
        try {
          Await.result(client.deleteBucket(bucketName), 15.seconds)
        } catch { case e: Exception => }
        client.close()
        throw e
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    try {
      Await.result(client.deleteBucket(bucketName), 15.seconds)
    } finally {
      client.close()
    }
  }

  protected def initBucket(bucket: BucketClient): Unit = {
    initFunc.foreach(_.apply(bucket))
  }

  protected def withFixture(test: Bucket => NoArgTest): Outcome = {
    withFixture(test(bucket))
  }

  def uniqueId = Random.nextInt().toHexString

  def withDocuments(docs: Map[String, DocumentDef]): Unit = {
    initFunc ::= { bc: BucketClient =>
      implicit val ec = client.system.dispatcher
      Await.result(Future.sequence(docs.map(d => bc.saveDesignDocument(d._1, d._2))), 10.seconds)
    }
  }

  def withDocuments(docs: (String, DocumentDef)*): Unit = withDocuments(docs.toMap)

  def waitForBucket(bucketName: String, retry: Int = 0, delay: Long = 100): Future[Bucket] = {
    implicit val ec = client.system.dispatcher

    if (retry > 10) Future.failed(new CouchbaseException(s"Couldn't get bucket: $bucketName"))
    else (client.actor ? GetBucket(bucketName)) flatMap {
      case b: Bucket if b.nodes.exists(_.healthy) => Future.successful(b)
      case _ => after(delay.millis, client.system.scheduler)(waitForBucket(bucketName, retry + 1, delay * 2))
    }
  }
}

trait BucketSpec extends CouchSpec { this: fixture.Suite =>

  type FixtureParam = Bucket
  protected def withFixture(test: OneArgTest): Outcome = super.withFixture(b => test.toNoArgTest(b))
}

