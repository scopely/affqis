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

import java.net.URI

import org.slf4j.{LoggerFactory, Logger}
import ws.wamp.jawampa.transport.SimpleWampWebsocketListener
import ws.wamp.jawampa.{WampRouter, WampRouterBuilder}

/**
 * Router management.
 */
case class JawampaRouter(server: SimpleWampWebsocketListener, router: WampRouter) {
  val log: Logger = LoggerFactory.getLogger(getClass)
  def destroy(): Unit = {
    log.debug("Cleaning up router/server...")
    server.stop()
    router.close()
  }
}

object JawampaRouter {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def addRealm(builder: WampRouterBuilder, realm: String): WampRouterBuilder =
    builder.addRealm(realm)

  def apply(realms: Seq[String]): JawampaRouter = {
    val routerBuilder: WampRouterBuilder = new WampRouterBuilder()
    val router: WampRouter = realms.foldLeft(routerBuilder) {addRealm}.build()
    val server: SimpleWampWebsocketListener =
      new SimpleWampWebsocketListener(router, URI.create(RouterConfig.url), null)

    log.info("Starting Jawampa server/router...")
    server.start()
    new JawampaRouter(server, router)
  }
}
