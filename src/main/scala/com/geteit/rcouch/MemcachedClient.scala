/**
 * Copyright (C) 2012 Klout Inc. <http://www.klout.com>
 */

package com.geteit.rcouch

import java.net.URLEncoder._

import akka.util.{ByteString, Timeout}
import akka.actor.{Props, ActorSystem}
import akka.pattern._
import scala.concurrent.duration._
import scala.concurrent.Future

/**
 * Asynchronous memcached client.
 */
trait MemcachedClient {

//    val DefaultTTL = 1 hour
//
//    /**
//     * Sets a single key - Fire and Forget
//     */
//    def set(key: String, value: String, ttl: Duration = DefaultTTL): Unit
//
//    /**
//     * Sets multiple key-value pairs, all with the same TTL - Fire and Forget
//     */
//    def mset(values: Map[String, String], ttl: Duration = DefaultTTL): Unit
//
//    /**
//     * Retrieves the value of a single key. In the case of a cache miss, this method will
//     * return a Future containing None. Otherwise, this method will return a Future of
//     * Some[T]
//     */
//    def get(key: String): Future[Option[String]]
//
//    /**
//     * Retrieves the values of multiple keys. This method returns a future of a mapping from
//     * cache keys to values. Keys that do not exist in Memcached will not be included in the
//     * map
//     */
//    def mget(keys: Set[String]): Future[Map[String, String]]
//
//    /**
//     * Deletes multiple keys - Fire and Forget
//     */
//    def delete(keys: String*): Unit

}

class RealMemcachedClient(hosts: List[(String, Int)], connectionsPerServer: Int = 1) extends MemcachedClient {

//    /**
//     * Maximum amount of time the client will wait for a response from
//     * a get instruction from Memcached
//     */
//    implicit val timeout = Timeout(30 seconds)
//
//    val system = ActorSystem()
//
//    val poolActor = system.actorOf(Props(new PoolActor(hosts, connectionsPerServer)), name = encode("Pool Actor", "UTF-8"))
//
//    override def set(key: String, value: String, ttl: Duration) {
//        mset(Map(key -> value), ttl)
//    }
//
//    override def mset(keyValueMap: Map[String, String], ttl: Duration) {
//        val serializedKeyValueMap = keyValueMap map {
//            case (key, value) => key -> ByteString(value)
//        }
//        poolActor ! SetCommand(serializedKeyValueMap, ttl.toSeconds)
//    }
//
//    override def get(key: String): Future[Option[String]] = {
//        mget(Set(key)).map(_.get(key))
//    }
//
//    override def mget(keys: Set[String]): Future[Map[String, String]] = {
//        val command = GetCommand(keys)
//        (poolActor ? command).map{
//            case GetResponse(results) => {
//                results.flatMap {
//                    case Found(key, value) => Some((key, value.toString))
//                    case NotFound(key)     => None
//                }
//            }.toMap
//            case other => throw new Exception("Invalid result returned: " + other)
//        }
//    }
//
//    override def delete(keys: String*) {
//        poolActor ! DeleteCommand(keys: _*)
//    }

}
