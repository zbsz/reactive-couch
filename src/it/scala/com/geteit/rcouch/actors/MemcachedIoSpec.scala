package com.geteit.rcouch.actors

import org.scalatest._
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap
import com.thimbleware.jmemcached.{Key, LocalCacheElement, CacheImpl, MemCacheDaemon}
import java.net.InetSocketAddress
import akka.actor.{LoggingFSM, Props, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.geteit.rcouch.Settings.MemcachedConfig
import com.geteit.rcouch.memcached.Memcached._
import com.geteit.rcouch.memcached.Memcached
import akka.util.ByteString

/**
  */
class MemcachedIoSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with FeatureSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MemcachedIoSpec"))

  val address = new InetSocketAddress("localhost", 21211)

  var daemon: MemCacheDaemon[LocalCacheElement] = _

  override protected def beforeAll() {
    super.beforeAll()

    // create daemon and start it
    daemon = new MemCacheDaemon[LocalCacheElement]

    val storage = ConcurrentLinkedHashMap.create[Key, LocalCacheElement](ConcurrentLinkedHashMap.EvictionPolicy.FIFO, 10000, 32 * 1024 * 1024)
    daemon.setCache(new CacheImpl(storage))
    daemon.setBinary(true)
    daemon.setAddr(address)
    daemon.setIdleTime(300)
    daemon.setVerbose(true)
    daemon.start()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    TestKit.shutdownActorSystem(system)
    daemon.stop()
  }

  feature("Connect to memcached server") {
    scenario("Connect to server and set value") {
      val memcached = system.actorOf(Props(classOf[MemcachedIoWithLogging], address, MemcachedConfig(authEnabled = false)))

      memcached ! Memcached.Set("key", ByteString("value"), 0, 3600)
      val res = expectMsgClass(classOf[StoreResponse])
      res.status should be(0)

      memcached ! Memcached.Get("key")
      val gr = expectMsgClass(classOf[GetResponse])
      gr.value should be(ByteString("value"))

      memcached ! Memcached.GetK("key")
      val gr1 = expectMsgClass(classOf[GetResponse])
      gr1.key should be(Some("key"))
      gr1.value should be(ByteString("value"))
    }
  }

  feature("Connect to Couchbase server") {
    scenario("Connect to server and set value") {
      val memcached = system.actorOf(Props(classOf[MemcachedIoWithLogging], new InetSocketAddress("localhost", 11210), MemcachedConfig(user = "geteit")))

      memcached ! Memcached.Set("key", ByteString("value"), 0, 3600)
      val res = expectMsgClass(classOf[StoreResponse])
      res.status should be(0)

      memcached ! Memcached.Get("key")
      val gr = expectMsgClass(classOf[GetResponse])
      gr.value should be(ByteString("value"))

      memcached ! Memcached.GetK("key")
      val gr1 = expectMsgClass(classOf[GetResponse])
      gr1.key should be(Some("key"))
      gr1.value should be(ByteString("value"))
    }
  }
}

private class MemcachedIoWithLogging(address: InetSocketAddress, config: MemcachedConfig)
    extends MemcachedIo(address, null: NodeRef, config) with LoggingFSM[MemcachedIo.State, MemcachedIo.Data]