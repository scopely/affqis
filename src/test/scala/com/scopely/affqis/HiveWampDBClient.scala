package com.scopely.affqis

import org.slf4j.{LoggerFactory, Logger}
import rx.lang.scala.Observable
import ws.wamp.jawampa.{Request, WampClient}
import rx.lang.scala.JavaConversions._

class HiveWampDBClient extends {
  val driver: String = "org.apache.hive.jdbc.HiveDriver"
  val realm: String = "hive"
  val jdbcPrefix: String = "hive2"
  val config: Map[String, String] = Map(
    "host" -> System.getenv("host"),
    "port" -> System.getenv("port")
  )
} with WampDBClient {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def apply(): Unit = {
    wampConnect { client: WampClient =>
      log.info("Registering JDBC executor")
      val proc: Observable[Request] = client.registerProcedure("start.doing.stuff")
      proc.subscribe { req: Request =>
        val response: java.lang.Long = req.arguments().get(0).asLong()
        req.reply(response)
      }
    }
  }
}
