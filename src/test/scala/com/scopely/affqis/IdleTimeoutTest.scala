package com.scopely.affqis


import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import rx.lang.scala.JavaConversions._
import rx.lang.scala.schedulers.NewThreadScheduler
import ws.wamp.jawampa.WampClient.Status
import ws.wamp.jawampa.{Reply, WampClient, WampClientBuilder}

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}

@RunWith(classOf[JUnitRunner])
class IdleTimeoutTest extends Specification {
  sequential

  trait IdleTimeoutScope extends WithRouterAndClient {
    val dbName = "IdleTimeoutDB"

    private def openConnection() = {
      val dbc = new {
        override val idleTimeout: Int = 3000
      } with EmbeddedDerbyWampDBClient()

      val connectPromise = Promise[Boolean]

      dbc {
        val nodeFactory: JsonNodeFactory = JsonNodeFactory.instance
        val scheduler: rx.Scheduler = NewThreadScheduler()
        val client: WampClient = new WampClientBuilder()
          .withUri("ws://localhost:8080/affqis")
          .withRealm("derby")
          .build()
        val status: rx.lang.scala.Observable[Status] =
          client.statusChanged().observeOn(scheduler)

        status.subscribe { status =>
          if (status == WampClient.Status.Connected) {
            val connectArgs = nodeFactory.objectNode()
            connectArgs.put("database", dbName)

            val connectResp: rx.lang.scala.Observable[Reply] =
              client.call("connect", nodeFactory.arrayNode(), connectArgs).observeOn(scheduler)
            connectResp.subscribe { _ => connectPromise success true }
          }
        }

        client.open()
      }

      Await.ready(connectPromise.future, 10 seconds)
      dbc
    }

    val dbc = openConnection()
  }

  "Timeouts should mean that connections idle out" >> new IdleTimeoutScope {
    dbc.connections must have size 1
    Thread.sleep(5000)
    dbc.connections must have size 0
  }
}

