package com.geteit.rcouch

/**
  */
object Settings {

  case class ClusterSettings(
                            bucketName: String,
                            hosts: List[String],
                            user: String = "",
                            passwd: String = "",
                            connection: ConnectionConfig = new ConnectionConfig(),
                            node: NodeConfig = new NodeConfig()
                          )

  case class NodeConfig(
    maxMemcachedConnections: Int = 1,
    memcached: MemcachedConfig = new MemcachedConfig()
  )

  case class MemcachedConfig(

    user: String = "",

    password: String = "",

    maxQueueSize: Int = 256,

    maxAuthRetries: Int = 3,

    authEnabled: Boolean = true,

    connection: ConnectionConfig = new ConnectionConfig()
  )

  case class ConnectionConfig(
    /**
     * The maximum amount of time that the client will sleep before trying to
     * reconnect to the Memcached server
     */
    maxReconnectDelayMillis: Int = 16000,

    /**
     * The maximum number of times that the client will try to reconnect to the memcached server
     * before aborting
     */
    maxReconnectAttempts: Int = 3
  )
}
