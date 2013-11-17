package com.geteit.rcouch.memcached

import com.geteit.rcouch.couchbase.Couchbase.VBucketMap

/**
  */
class VBucketLocator(map: VBucketMap) {

  import VBucketLocator._

  val hash = HashAlgorithm(map.hashAlgorithm)
  val servers = map.serverList.map(s => s.substring(0, s.indexOf(':')))
  val vbuckets = map.vBucketMap.map(arr => VBucket(servers(arr(0)), arr.drop(1).filter(_ >= 0).map(servers)))
  val mask = map.vBucketMap.length - 1

  def hasVBuckets: Boolean = !vbuckets.isEmpty

  def vbucketIndex(key: String): Int = hash(key).toInt & mask

  def apply(key: String) = vbuckets(vbucketIndex(key))
}

object VBucketLocator {

  case class VBucket(primary: String, replicas: Array[String] = Array())

}