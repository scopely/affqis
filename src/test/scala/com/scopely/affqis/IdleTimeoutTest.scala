package com.scopely.affqis

import java.util.concurrent.CountDownLatch

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import rx.lang.scala.schedulers.NewThreadScheduler
import rx.lang.scala.JavaConversions._
import ws.wamp.jawampa.WampClient.Status
import ws.wamp.jawampa.{Reply, WampClientBuilder, WampClient}

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class IdleTimeoutTest extends Specification {
  JawampaRouter(Seq("derby"))
  val latch = new CountDownLatch(1)
  val dbc = new {
    override val idleTimeout: Int = 3000
  } with EmbeddedDerbyWampDBClient()

  dbc {
    val nodeFactory: JsonNodeFactory = JsonNodeFactory.instance
    val scheduler: rx.Scheduler = NewThreadScheduler()
    val client: WampClient = new WampClientBuilder()
      .withUri("ws://localhost:8080/affqis")
      .withRealm("derby")
      .build()
    val status: rx.lang.scala.Observable[Status] =
      client.statusChanged().observeOn(scheduler)

    val connectPromise = Promise[Boolean]

    status.subscribe { status =>
      if (status == WampClient.Status.Connected) {
        val connectArgs = nodeFactory.objectNode()
        connectArgs.put("database", "IdleTimeoutTest")

        val connectResp: rx.lang.scala.Observable[Reply] =
          client.call("connect", nodeFactory.arrayNode(), connectArgs).observeOn(scheduler)
        connectResp.subscribe { _ => connectPromise success true }
      }
    }

    client.open()

    "Timeouts should mean that" >> {
      Await.ready(connectPromise.future, 30 seconds)

      "there will be a connection at first" >> {
        dbc.connections must be size(1)
      }

      "after 5 seconds, our connection list is empty" >> {
        Thread.sleep(5000)
        dbc.connections must be size(0)
      }
    }

    latch.countDown()
  }

  latch.await()
}

