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
