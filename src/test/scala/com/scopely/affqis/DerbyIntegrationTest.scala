package com.scopely.affqis

import java.util.UUID

import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import org.junit.runner.RunWith
import org.specs2.mutable.{After, Specification}
import org.specs2.runner.JUnitRunner
import rx.lang.scala.JavaConversions._
import rx.lang.scala.schedulers.NewThreadScheduler
import ws.wamp.jawampa.WampClient.Status
import ws.wamp.jawampa.{PubSubData, Reply, WampClient, WampClientBuilder}

import scala.concurrent.{Await, Promise}
import scala.util.Try
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class DerbyIntegrationTest extends Specification {
  sequential

  // Connection ID
  val connectPromise = Promise[Try[UUID]]
  val connectResponse = connectPromise.future

  // Execution results
  val executePromise = Promise[Seq[String]]
  val executeResponse = executePromise.future

  // Disconnect response
  val disconnectPromise = Promise[Boolean]
  val disconnectResponse = disconnectPromise.future

  trait DerbyIntegrationScope extends WithRouterAndClient {
    val dbName = "InternalTestDb"

    def openConnection() = {
      val dbc = new EmbeddedDerbyWampDBClient()
      dbc {
        val nodeFactory: JsonNodeFactory = JsonNodeFactory.instance
        val scheduler: rx.Scheduler = NewThreadScheduler()
        val client: WampClient = new WampClientBuilder()
          .withUri("ws://localhost:8080/affqis")
          .withRealm("derby")
          .build()
        val status: rx.lang.scala.Observable[Status] = client.statusChanged().observeOn(scheduler)

        status.subscribe { status =>
          if (status == WampClient.Status.Connected) {
            val connectArgs: ObjectNode = nodeFactory.objectNode()
            connectArgs.put("database", dbName)
            val connectResult: rx.lang.scala.Observable[Reply] =
              client.call("connect", nodeFactory.arrayNode(), connectArgs).observeOn(scheduler)
            connectResult.subscribe { result =>
              val uuidString = result.arguments().get(0).asText()

              // SHIP IT
              connectPromise success Try(UUID.fromString(uuidString))

              val executeArgs = nodeFactory.objectNode()
              executeArgs.put("connectionId", uuidString)
              executeArgs.put("sql", "VALUES 1, 2, 3")
              val executeResult: rx.lang.scala.Observable[Reply] =
                client.call("execute", nodeFactory.arrayNode(), executeArgs).observeOn(scheduler)
              executeResult.subscribe { result =>
                val args = result.arguments()
                val event = args.get(0).asText()
                val streamProc = args.get(1).asText()
                var sqlResults: Seq[String] = Seq()

                val executionSub: rx.lang.scala.Observable[PubSubData] = client.makeSubscription(event)
                executionSub.subscribe { result =>
                  val args = result.arguments()
                  if (args.size() > 1) {
                    sqlResults = sqlResults :+ args.get(1).asText()
                  } else {
                    executePromise success sqlResults

                    val disconnectArgs = nodeFactory.objectNode()
                    disconnectArgs.put("connectionId", uuidString)
                    val disconnectResult: rx.lang.scala.Observable[Reply] =
                      client.call("disconnect", nodeFactory.arrayNode(), disconnectArgs).observeOn(scheduler)
                    disconnectResult.subscribe { result =>
                      disconnectPromise success result.arguments().get(0).asBoolean()
                    }
                  }
                }

                client.call(streamProc)
              }
            }
          }
        }

        client.open()
      }
      Await.result(connectResponse, 10 seconds) must beSuccessfulTry
      dbc
    }

    val dbc = openConnection()
  }

  "Client must be able to execute sql and get results" >> new DerbyIntegrationScope {
    val result = Await.result(executeResponse, 25 seconds)
    result must be size 3
    result must contain(
      """[{"value":1,"type":"INTEGER","name":"1"}]""",
      """[{"value":2,"type":"INTEGER","name":"1"}]""",
      """[{"value":3,"type":"INTEGER","name":"1"}]"""
    )

    Await.result(disconnectResponse, 10 seconds) must beTrue
  }
}
