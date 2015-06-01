package com.scopely.affqis

import java.util.UUID

import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import rx.lang.scala.JavaConversions._
import rx.lang.scala.schedulers.NewThreadScheduler
import ws.wamp.jawampa.WampClient.Status
import ws.wamp.jawampa.{Reply, WampClient, WampClientBuilder}

import scala.concurrent.{Await, Promise}
import scala.util.Try
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class DerbyIntegrationTest extends Specification {
  JawampaRouter(Seq("derby"))
  new EmbeddedDerbyWampDBClient()({

    val nodeFactory: JsonNodeFactory = JsonNodeFactory.instance
    val scheduler: rx.Scheduler = NewThreadScheduler()

    val client: WampClient = new WampClientBuilder()
      .withUri("ws://localhost:8080/affqis")
      .withRealm("derby")
      .build()

    val status: rx.lang.scala.Observable[Status] = client.statusChanged().observeOn(scheduler)
    val connectPromise = Promise[Try[UUID]]
    val connectResponse = connectPromise.future
    status.subscribe { status =>
      if (status == WampClient.Status.Connected) {
        val objectNode: ObjectNode = nodeFactory.objectNode()
        objectNode.put("database", "InternalTestDb")
        val result: rx.lang.scala.Observable[Reply] =
          client.call("connect", nodeFactory.arrayNode(), objectNode).observeOn(scheduler)
        result.subscribe { result =>
          println(result)
          connectPromise success Try(UUID.fromString(result.arguments().get(0).asText()))
        }
      }
    }

    client.open()

    "Client must" >> {
      "connect via JDBC and return a UUID" >> {
        Await.result(connectResponse, 10 seconds) must beSuccessfulTry
      }
    }
  })
}
