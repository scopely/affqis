package com.scopely.affqis

import scala.util.Try

/**
 * Configuration from environment variables.
 */
object RouterConfig {
  val port: Int = Try(Integer.parseInt(System.getenv("PORT"))).getOrElse(8080)
  val host: String = Option(System.getenv("HOST")).getOrElse("0.0.0.0")
  val url: String = s"ws://$host:$port/affqis"
}
