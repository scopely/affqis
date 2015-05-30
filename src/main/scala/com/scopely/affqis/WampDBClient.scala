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
import java.sql._
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.{Logger, LoggerFactory}
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Subscription
import rx.lang.scala.schedulers._
import ws.wamp.jawampa.WampClient.Status
import ws.wamp.jawampa.{Request, ApplicationError, WampClient, WampClientBuilder}

import scala.collection.concurrent
import scala.reflect.{ClassTag, classTag}
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Base trait for DB clients.
 */
trait WampDBClient {
  def driver: String
  def realm: String

  private val log: Logger = LoggerFactory.getLogger(getClass)
  val scheduler: rx.Scheduler = NewThreadScheduler()
  
  /**
   * jdbc:<prefix>://...
   */
  def jdbcPrefix: String

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
   * Map of session ids to connections.
   * @todo Probably don't want to just assume these will be cleaned up by clients.
   *       A possible solution is to have an idle time out and if the connection
   *       isn't used or acked or something, fry it.
   */
  val connections: concurrent.Map[String, Connection] = new ConcurrentHashMap[String, Connection]().asScala

  type ArgSpec = Map[String, Class[_]]

  /**
   * Validates that args exist and are of the correct type.
   */
  def hasArgs[A: ClassTag](arg: ObjectNode, expected: ArgSpec): Boolean = {
    expected.forall { case (argName: String, argType: Class[A @unchecked]) =>
      if (arg.has(argName)) {
        argType match {
          case _: Class[String @unchecked] if classTag[A] == classTag[String] => true
          case _: Class[Int @unchecked] if classTag[A] == classTag[Int] => arg.get(argName).canConvertToInt
          case _: Class[Long @unchecked] if classTag[A] == classTag[Long] => arg.get(argName).canConvertToLong
          case _ => false // I DUNNO WHAT'S HAPPENING
        }
      } else {
        false
      }
    }
  }

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

      val database: String = if (args.has("database")) {
        args.get("database").asText()
      } else {
        "default"
      }

      // Passwords aren't required all the time.
      val pass: String = if (args.has("password")) {
        args.get("password").asText()
      } else {
        ""
      }

      val id: String = UUID.randomUUID().toString

      log.info(s"Creating a new connection: $id")
      val connection: Try[Connection] = Try(connectJdbc(user, host, port, database, pass))
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
      log.info(s"Sending update count to $event")

      // This is ugh but the type system doesn't much care for publish being overloaded
      // and us passing a combination of Strings and Ints.
      client.publish(event, "update_count", statement.getUpdateCount.asInstanceOf[Object])
      ()
    } { rs: ResultSet =>
      log.info(s"Sending rows to $event")
      JsonResults(rs).foreach { row: String => client.publish(event, "row", row) }
      rs.close()
    }

    log.info(s"Finished sending results to $event. Letting subscribers know")
    client.publish(event, "finished")
    req.reply()

    log.debug(s"Cleaning up $event")
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
        case exn: Exception =>
          val trace = ExceptionUtils.getTrace(exn)
          log.error(trace)
          req.replyError("execution_error", id, trace)
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
      log.info(s"Registering $realm JDBC connect procedure")
      client.registerProcedure("connect").subscribe(connectProc(client) _)

      log.info(s"Registering $realm JDBC execute procedure")
      client.registerProcedure("execute").subscribe(executeProc(client) _)

      log.info(s"Registering $realm JDBC disconnect procedure")
      client.registerProcedure("disconnect").subscribe(disconnectProc(client) _)
    }
  }
}
