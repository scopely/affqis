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

import java.math.BigInteger
import java.sql.{Connection, ParameterMetaData, PreparedStatement, Types}

import com.fasterxml.jackson.databind
import com.fasterxml.jackson.databind.JsonNode

object SqlParams {
  def apply(sql: String, params: Seq[JsonNode], connection: Connection): PreparedStatement = {
    val statement: PreparedStatement = connection.prepareStatement(sql)
    val meta: ParameterMetaData = statement.getParameterMetaData
    params.zipWithIndex.foreach { case (param: databind.JsonNode, i: Int) =>
      val index: Int = i + 1
      val paramType: Int = meta.getParameterType(index)
      paramType match {
          case Types.BIGINT => statement.setObject(index, param.asInstanceOf[BigInteger], Types.BIGINT)
          case Types.BOOLEAN => statement.setBoolean(index, param.asBoolean())
          case Types.DATE => statement.setDate(index, java.sql.Date.valueOf(param.asText()))
          case Types.TIMESTAMP => statement.setTimestamp(index, java.sql.Timestamp.valueOf(param.asText()))
          case Types.TIME => statement.setTime(index, java.sql.Time.valueOf(param.asText()))
          case Types.DECIMAL | Types.FLOAT | Types.DOUBLE | Types.NUMERIC | Types.REAL =>
            statement.setObject(index, param.asDouble(), paramType)
          case Types.INTEGER | Types.SMALLINT | Types.TINYINT =>
            statement.setObject(index, param.asInt(), paramType)
          case _ => statement.setObject(index, param.asText())
      }
    }

    statement
  }
}
