package com.scopely.affqis

import java.sql.{DriverManager, Connection}

import com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.{LoggerFactory, Logger}
import ws.wamp.jawampa.{WampClient, Request}

class EmbeddedDerbyWampDBClient extends {
  val driver: String = "org.apache.hive.jdbc.HiveDriver"
  val realm: String = "derby"
} with WampDBClient {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  case class DerbyDBConnection(connection: Connection, dbName: String) extends DBConnection

  def handleConnect(client: WampClient)(req: Request): Unit = {
    val args: ObjectNode = req.keywordArguments()

    if (hasArgs(args, Map("database" -> classOf[String]))) {
      val database: String = args.get("database").asText()
      registerConnection(req) {
        val connection: Connection =
          DBConnection.connect(s"jdbc:derby:$database;create=true")
        new DerbyDBConnection(connection, database)
      }
    } else invalidArgs(req)
  }

  def onDisconnect(id: String, conn: DerbyDBConnection): Unit = {
    val database: String = conn.dbName
    log.info(s"Shutting down derby database $database")
    DriverManager.getConnection(s"jdbc:derby:$database;shutdown=true")
  }
}
