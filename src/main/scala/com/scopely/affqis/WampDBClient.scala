/*
 *    Copyright 2015 Scopely
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.scopely.affqis

import java.sql.{DriverManager, Connection}
import java.util.concurrent.TimeUnit

import org.slf4j.{Logger, LoggerFactory}
import rx.lang.scala.schedulers._
import rx.lang.scala.JavaConversions._
import ws.wamp.jawampa.WampClient.Status
import ws.wamp.jawampa.{WampClientBuilder, WampClient}

/**
 * Base trait for DB clients.
 */
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

  // Initialize our driver.
  Class forName driver

  log.info(s"Creating a WAMP client for realm $realm, uri ${RouterConfig.url}")
  val wampClient: WampClient = new WampClientBuilder()
    .withUri(RouterConfig.url)
    .withRealm(realm)
    .withInfiniteReconnects()
    .withReconnectInterval(3, TimeUnit.SECONDS)
    .build()

  /**
   * Connect via JDBC to a database.
   */
  def connectJdbc(user: String, host: String, port: Int, db: String, pass: String = ""): Connection = {
    val jdbcURI: String =
      s"jdbc:$jdbcPrefix://$host:$port/$db"
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
