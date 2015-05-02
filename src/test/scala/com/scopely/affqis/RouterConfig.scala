package com.scopely.affqis

/**
 * Configuration from environment variables.
 */
object RouterConfig {
  val port: Int = Integer.parseInt(System.getenv("PORT"))
  val host: String = System.getenv("HOST")
  val path: String = System.getenv("PATH")
  val url: String = s"ws://$host:$port/$path"
}
