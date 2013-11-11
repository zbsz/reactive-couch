package com.geteit.rcouch.memcached

import java.security.MessageDigest
import java.util.zip.CRC32

/**
 * Known hashing algorithms for locating a server for a key. Note that all hash
 * algorithms return 64-bits of hash, but only the lower 32-bits are
 * significant. This allows a positive 32-bit number to be returned for all
 * cases.
 */
object HashAlgorithm {

  type Algorithm = String => Long

  /**
   * Native hash (String.hashCode()).
   */
  val NATIVE_HASH = "NATIVE"
  /**
   * CRC_HASH as used by the perl API. This will be more consistent both
   * across multiple API users as well as java versions, but is mostly likely
   * significantly slower.
   */
  val CRC_HASH = "CRC"
  /**
   * FNV hashes are designed to be fast while maintaining a low collision rate.
   * The FNV speed allows one to quickly hash lots of data while maintaining a
   * reasonable collision rate.
   *
   * @see <a href="http://www.isthe.com/chongo/tech/comp/fnv/">fnv comparisons</a>
   * @see <a href="http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash">fnv at wikipedia</a>
   */
  val FNV1_64_HASH = "FNV1_64"
  /**
   * Variation of FNV.
   */
  val FNV1A_64_HASH = "FNV1A_64"
  /**
   * 32-bit FNV1.
   */
  val FNV1_32_HASH = "FNV1_32"
  /**
   * 32-bit FNV1a.
   */
  val FNV1A_32_HASH = "FNV1A_32"
  /**
   * MD5-based hash algorithm used by ketama.
   */
  val KETAMA_HASH = "KETAMA"

  private val FNV_64_INIT = 0xcbf29ce484222325L
  private val FNV_64_PRIME = 0x100000001b3L
  private val FNV_32_INIT = 2166136261L
  private val FNV_32_PRIME = 16777619
  
  private val md5Digest = MessageDigest.getInstance("MD5")
  
  /**
   * Get the md5 of the given key.
   */
  def computeMd5(k: String): Array[Byte] = {
    val md5 = md5Digest.clone.asInstanceOf[MessageDigest]
    md5.update(k.getBytes("utf8"))
    md5.digest
  }

  val algorithms: Map[String, Algorithm] = Map(
    NATIVE_HASH -> ((_: String).hashCode & 0xffffffffL),
    CRC_HASH -> { k: String =>
      val crc32 = new CRC32()
      crc32.update(k.getBytes("utf8"))
      (crc32.getValue >> 16) & 0x7fff 
    },
    FNV1_64_HASH -> { (_: String).foldLeft(FNV_64_INIT)((hash, c) => hash * FNV_64_PRIME ^ c) & 0xffffffffL },
    FNV1A_64_HASH -> { (_: String).foldLeft(FNV_64_INIT)((hash, c) => hash ^ c * FNV_64_PRIME) & 0xffffffffL },
    FNV1_32_HASH -> { (_: String).foldLeft(FNV_32_INIT)((hash, c) => hash * FNV_32_PRIME ^ c) & 0xffffffffL },
    FNV1A_32_HASH -> { (_: String).foldLeft(FNV_32_INIT)((hash, c) => hash ^ c * FNV_32_PRIME) & 0xffffffffL },
    KETAMA_HASH -> { k: String =>
      val bKey = computeMd5(k)
      (((bKey(3) & 0xFF).asInstanceOf[Long] << 24) | 
        ((bKey(2) & 0xFF).asInstanceOf[Long] << 16) | 
        ((bKey(1) & 0xFF).asInstanceOf[Long] << 8) | 
        (bKey(0) & 0xFF)) & 0xffffffffL
    }
  )
  
  def apply(algorithm: String) = algorithms(algorithm)  
}