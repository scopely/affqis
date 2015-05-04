package com.scopely.affqis

import java.net.URI

import org.slf4j.{LoggerFactory, Logger}
import ws.wamp.jawampa.transport.SimpleWampWebsocketListener
import ws.wamp.jawampa.{WampRouter, WampRouterBuilder}

/**
 * Router management.
 */
case class JawampaRouter(server: SimpleWampWebsocketListener, router: WampRouter)

object JawampaRouter {
  val log: Logger = LoggerFactory.getLogger(getClass)
  val routerBuilder: WampRouterBuilder = new WampRouterBuilder()

  def addRealm(builder: WampRouterBuilder, realm: String): WampRouterBuilder =
    builder.addRealm(realm)

  def apply(realms: Seq[String]): JawampaRouter = {
    val router: WampRouter = realms.foldLeft(routerBuilder) {addRealm}.build()
    val server: SimpleWampWebsocketListener =
      new SimpleWampWebsocketListener(router, URI.create(RouterConfig.url), null)

    log.info("Starting Jawampa server/router...")
    server.start()
    new JawampaRouter(server, router)
  }
}
