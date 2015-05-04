package com.scopely.affqis

import java.sql.{Connection, ResultSet, SQLException}
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import com.fasterxml.jackson.databind.node.ArrayNode
import org.slf4j.{Logger, LoggerFactory}
import rx.lang.scala.JavaConversions._
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
    val args: ArrayNode = req.arguments()

    if (args.size() >= 3 && args.get(2).canConvertToInt) {
      val user: String = args.get(0).asText()
      val host: String = args.get(1).asText()
      val port: Int = args.get(2).asInt()
      val database: String = if (args.size() > 3) args.get(3).asText() else "default"

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
   * Procedure function for executing SQL and streaming results. Returns an event URI
   * the client can subscribe to for results.
   */
  def executeProc(client: WampClient)(req: Request): Unit = {
    val args: ArrayNode = req.arguments()

    if (args.size() < 2) {
      req.replyError(new ApplicationError(ApplicationError.INVALID_ARGUMENT))
    } else {
      val connectionId: String = args.get(0).asText()
      val connection: Connection = connections(connectionId)
      val sql: String = args.get(1).asText()

      log.info(s"Executing SQL on connection $connectionId")

      // Hive currently doesn't support parameter metadata so the hell with this.
      // val params: Seq[databind.JsonNode] = args.iterator().asScala.toSeq.drop(2)
      val id: String = UUID.randomUUID().toString
      val event: String = "execution." + id.replace('-', '_')

      val rs: Try[ResultSet] = Try(connection.prepareStatement(sql).executeQuery())

      rs.map { rs: ResultSet =>
        req.reply(event)

        JsonResults(rs).foreach { row: String => client.publish(event, "row", row) }
        client.publish(event, "finished")
        rs.close()
      } recover {
        case exn: SQLException =>
          req.replyError("execution_error", id, exn.getMessage)
      }
    }
  }

  /**
   * Procedure function for closing a jdbc connection by id.
   */
  def disconnectProc(client: WampClient)(req: Request): Unit = {
    val args: ArrayNode = req.arguments()

    if (args.size() == 1) {
      val id = args.get(0).asText()

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
