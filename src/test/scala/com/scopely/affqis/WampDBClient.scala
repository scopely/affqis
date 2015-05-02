package com.scopely.affqis

import java.sql.{DriverManager, Connection}
import java.util.concurrent.TimeUnit

import org.slf4j.{Logger, LoggerFactory}
import rx.lang.scala.schedulers._
import rx.lang.scala.JavaConversions._
import ws.wamp.jawampa.WampClient.Status
import ws.wamp.jawampa.{WampClientBuilder, WampClient}

trait WampDBClient {
  def driver: String
  def realm: String
  def config: Map[String, String]

  private val log: Logger = LoggerFactory.getLogger(getClass)
  val scheduler: rx.Scheduler = NewThreadScheduler()
  
  /**
   * jdbc:<prefix>://...
   */
  val jdbcPrefix: String

  val jdbcURI: String =
    s"jdbc:$jdbcPrefix://${config("host")}:${config("port")}/:${config.getOrElse("db", "")}"

  // Initialize our driver.
  Class forName driver

  log.info(s"Creating a WAMP client for realm $realm, uri ${RouterConfig.url}")
  val wampClient: WampClient = new WampClientBuilder()
    .withUri(RouterConfig.url)
    .withRealm(realm)
    .withInfiniteReconnects()
    .withReconnectInterval(3, TimeUnit.SECONDS)
    .build()

  def connect(user: String, pass: String = ""): Connection = {
    log.info(s"Establishing a connection to $jdbcURI")
    DriverManager.getConnection(jdbcURI, user, pass)
  }

  /**
   * Set up our callback when this guy is connected.
   */
  def wampConnect(callback: WampClient => Unit) = {
    val status: rx.lang.scala.Observable[Status] = wampClient.statusChanged().observeOn(scheduler)
    status.subscribe { status =>
      log.debug(s"Connection status changed to $status")
      if (status == WampClient.Status.Connected) {
        log.debug("Connected! Running callback...")
        callback(wampClient)
      }
    }
    wampClient.open()
  }
}
