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

import java.io.{PrintWriter, StringWriter}
import java.sql.{PreparedStatement, Connection, ResultSet, SQLException}
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.{Logger, LoggerFactory}
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Subscription
import ws.wamp.jawampa.{ApplicationError, Request, WampClient}

import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.util.Try

/**
 * Implementation of WampDBClient for HiveServer2.
 * @todo Most of this can be moved to WampDBClient.
 */
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

  /**
   * Map of session ids to connections.
   * @todo Probably don't want to just assume these will be cleaned up by clients.
   *       A possible solution is to have an idle time out and if the connection
   *       isn't used or acked or something, fry it.
   */
  val connections: concurrent.Map[String, Connection] = new ConcurrentHashMap[String, Connection]().asScala

  /**
   * Procedure function to connect to the database. Returns an id to the client that can be
   * used to execute and close this connection later.
   */
  def connectProc(client: WampClient)(req: Request): Unit = {
    val args: ObjectNode = req.keywordArguments()
    val argSpec: ArgSpec = Map(
      "user" -> classOf[String],
      "port" -> classOf[Int],
      "host" -> classOf[String]
    )

    if (hasArgs(args, argSpec)) {
      val user: String = args.get("user").asText()
      val host: String = args.get("host").asText()
      val port: Int = args.get("port").asInt()
      val database: String = if (args.has("database")) args.get("database").asText() else "default"

      val id: String = UUID.randomUUID().toString

      log.info(s"Creating a new connection: $id")
      val connection: Try[Connection] = Try(connectJdbc(user, host, port, database))
      connection.map { conn: Connection =>
        connections += (id -> conn)
        req.reply(id)
        conn
      } recover { case exn: SQLException =>
        req.replyError("connect_error", exn.getMessage)
      }
    } else {
      req.replyError(new ApplicationError(ApplicationError.INVALID_ARGUMENT))
    }
  }

  /**
   * Procedure for streaming the results of a successful query to a client. The idea
   * is that `executeProc` registers a unique procedure with this function and sends
   * the proc ID to the client along with the event this guy will stream to.
   */
  def streamResults(statement: PreparedStatement, event: String,
                    proc: Subscription, client: WampClient)
                   (req: Request): Unit = {

    val rs: Option[ResultSet] = Option(statement.getResultSet)

    rs.fold {
      log.info(s"Sending update count to ${event}")

      // This is ugh but the type system doesn't much care for publish being overloaded
      // and us passing a combination of Strings and Ints.
      client.publish(event, "update_count", statement.getUpdateCount.asInstanceOf[Object])
      ()
    } { rs: ResultSet =>
      log.info(s"Sending rows to ${event}")
      JsonResults(rs).foreach { row: String => client.publish(event, "row", row) }
      rs.close()
    }

    log.info(s"Finished sending results to ${event}. Letting subscribers know")
    client.publish(event, "finished")
    req.reply()

    log.debug(s"Cleaning up ${event}")
    proc.unsubscribe()
  }

  /**
   * Procedure function for executing SQL and streaming results. Replies with an
   * event URI and a proc URI (two arguments) after the query runs. The client
   * subscribes to the event and _then_ calls the procedure (unique to this
   * query) to initiate streaming of results.
   *
   * Ideally we'd just wait for our subscriber before we stream rows, but it seems
   * that until Jawampa supports Wamp V2 Advanced Profile, there is no way to get
   * subscribers, so we can't even poll for this!
   */
  def executeProc(client: WampClient)(req: Request): Unit = {
    val args: ObjectNode = req.keywordArguments()
    val argSpec: ArgSpec = Map(
      "connectionId" -> classOf[String],
      "sql" -> classOf[String]
    )

    if (hasArgs(args, argSpec)) {
      val connectionId: String = args.get("connectionId").asText()
      val connection: Connection = connections(connectionId)
      val sql: String = args.get("sql").asText()

      log.info(s"Executing SQL on connection $connectionId")

      // Hive currently doesn't support parameter metadata so the hell with this.
      // val params: Seq[databind.JsonNode] = args.iterator().asScala.toSeq.drop(2)
      val id: String = UUID.randomUUID().toString.replace('-', '_')
      val event: String = "results." + id
      val proc: String = "stream_results." + id

      val statement: PreparedStatement = connection.prepareStatement(sql)

      val result: Try[Boolean] = Try(statement.execute())

      result.map { wasQuery: Boolean =>
        // We're making sub lazy because we're passing it to a function in the definition
        // itself, which causes a compile error because it's a forward reference at that point.
        // Making it lazy and then immediately realizing it seems to be the way with the least
        // amount of indirection to solve this issue. Could use promises or observables, I
        // guess, but ugh.
        lazy val sub: Subscription = client
          .registerProcedure(proc)
          .subscribe(streamResults(statement, event, sub, client) _)
        sub

        req.reply(event, proc)
      } recover {
        case exn: SQLException =>
          val traceStringWriter: StringWriter = new StringWriter()
          val traceWriter: PrintWriter = new PrintWriter(traceStringWriter, true)
          exn.printStackTrace(traceWriter)
          val trace = traceStringWriter.getBuffer.toString
          log.error(trace)
          req.replyError("execution_error", id, trace)
      }
    } else {
      req.replyError(new ApplicationError(ApplicationError.INVALID_ARGUMENT))
    }
  }

  /**
   * Procedure function for closing a jdbc connection by id.
   */
  def disconnectProc(client: WampClient)(req: Request): Unit = {
    val args: ObjectNode = req.keywordArguments()
    val argSpec: ArgSpec = Map(
      "connectionId" -> classOf[String]
    )

    if (hasArgs(args, argSpec)) {
      val id = args.get("connectionId").asText()

      connections.get(id).fold {
        req.reply(java.lang.Boolean.valueOf("false"))
      } { conn: Connection =>
        log.info(s"Closing connection for id $id")
        conn.close()
        req.reply(java.lang.Boolean.valueOf("true"))
      }
    } else {
      req.replyError(new ApplicationError(ApplicationError.INVALID_ARGUMENT))
    }
  }

  def apply(): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread {
      log.info("Closing all open connections...")
      connections.values foreach(_.close())
    })

    wampConnect { client: WampClient =>
      log.info("Registering Hive JDBC connect procedure")
      client.registerProcedure("connect").subscribe(connectProc(client) _)

      log.info("Registering Hive JDBC execute procedure")
      client.registerProcedure("execute").subscribe(executeProc(client) _)

      log.info("Registering Hive JDBC disconnect procedure")
      client.registerProcedure("disconnect").subscribe(disconnectProc(client) _)
    }
  }
}
