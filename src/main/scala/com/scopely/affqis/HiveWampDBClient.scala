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

import java.sql.Connection

import com.fasterxml.jackson.databind.node.ObjectNode
import ws.wamp.jawampa.{WampClient, Request}

/**
 * Implementation of WampDBClient for HiveServer2.
 */
class HiveWampDBClient extends {
  val driver: String = "org.apache.hive.jdbc.HiveDriver"
  val realm: String = "hive"
} with WampDBClient {

  case class HiveDBConnection(connection: Connection) extends DBConnection
  def handleConnect(client: WampClient)(req: Request): Unit = {
    val args: ObjectNode = req.keywordArguments()

    if (hasArgs(args, Map(
      "user" -> classOf[String],
      "port" -> classOf[Int],
      "host" -> classOf[String]
    ))) {
      val user: String = args.get("user").asText()
      val host: String = args.get("host").asText()
      val port: Int = args.get("port").asInt()
      val database: String = Option(args.get("database")) map {_.asText()} getOrElse "default"
      val pass: String = Option(args.get("password")) map {_.asText()} getOrElse ""

      val jdbcURI: String = s"jdbc:hive2://$host:$port/$database"

      registerConnection(req) { new HiveDBConnection(connectJdbc(jdbcURI, user, pass)) }
    } else invalidArgs(req)
  }
}
