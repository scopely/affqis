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

import java.sql.{ResultSet, ResultSetMetaData}
import java.sql.Types._
import java.math.{BigInteger, BigDecimal}

import spray.json._

/**
 * Provides a convenient API to produce JSON from a ResultSet, with type metadata info.
 * @param rs A result set.
 */
object JsonResults {
  /**
   * Converts an object to a JsValue using the specified function. If obj is null, JsNull is returned.
   * @param obj Some kind of object.
   * @param jsType Type converter function.
   * @tparam T
   * @return a JsValue for the value.
   */
  def toJsVal[T](obj: T, jsType: T => JsValue): JsValue = {
    Option(obj) map(jsType(_)) getOrElse JsNull
  }

  /**
   * Given a column index, convert that column's value into a JsValue.
   * @param colIndex index of a column
   * @return a JsValue
   */
  def jsonColumnVal(rs: ResultSet, meta: ResultSetMetaData)(colIndex: Int): JsValue = {
    val colType: Int = meta.getColumnType(colIndex)

    val jsonCol: JsValue = colType match {
        case DECIMAL | DOUBLE  | FLOAT | NUMERIC | REAL =>
          toJsVal(rs.getBigDecimal(colIndex), JsNumber(_: BigDecimal))
        case BIGINT => toJsVal(rs.getLong(colIndex), JsNumber(_: Long))
        case INTEGER | TINYINT | SMALLINT =>
          toJsVal(rs.getInt(colIndex), JsNumber(_: Int))

        case BOOLEAN => toJsVal(rs.getBoolean(colIndex), JsBoolean(_: Boolean))
        case _ => toJsVal(rs.getString(colIndex), JsString(_: String))
    }

    JsObject(
      "value" -> jsonCol,
      "type" -> JsString(meta.getColumnTypeName(colIndex)),
      "name" -> JsString(meta.getColumnName(colIndex))
    )
  }

  /**
   * Get an iterator over a ResultSet where each value is a JSON string.
   * @return A row -> json iterator.
   */
  def apply(rs: ResultSet): Iterator[String] = {
    val meta: ResultSetMetaData = rs.getMetaData
    val colCount: Integer = meta.getColumnCount
    val results: ResultSetIterator = new ResultSetIterator(rs)
    results.map { cursor: ResultSet =>
      JsArray((1 to colCount).map(jsonColumnVal(rs, meta)): _*).compactPrint
    }
  }
}
