package com.scopely.affqis

import java.sql.{ResultSet, ResultSetMetaData}
import java.sql.Types._

import spray.json._
import DefaultJsonProtocol._

/**
 * Provides a convenient API to produce JSON from a ResultSet, with type metadata info.
 * @param rs A result set.
 */
class JsonResults(rs: ResultSet) {
  val meta: ResultSetMetaData = rs.getMetaData
  val colCount: Integer = meta.getColumnCount

  /**
   * A map of column names to their type names.
   */
  val types: Map[String, String] = (1 to colCount) map { col: Int =>
    meta.getColumnName(col) -> meta.getColumnTypeName(col)
  } toMap

  /**
   * Given a column index, convert that column's value into a JsValue.
   * @param colIndex index of a column
   * @return a JsValue
   */
  def jsonColumnVal(colIndex: Int): JsValue = {
    val colType: Int = meta.getColumnType(colIndex)

    val jsonCol: JsValue = colType match {
        case DECIMAL | DOUBLE  | FLOAT => Option(rs.getBigDecimal(colIndex)) map(JsNumber(_)) getOrElse JsNull
        case BIGINT  | INTEGER | TINYINT => Option(rs.getInt(colIndex)) map(JsNumber(_)) getOrElse JsNull
        case BOOLEAN => Option(rs.getBoolean(colIndex)) map(JsBoolean(_)) getOrElse JsNull
        case _ => Option(rs.getString(colIndex)) map(JsString(_)) getOrElse JsNull
    }

    jsonCol
  }

  /**
   * Get an iterator over a ResultSet where each value is a JSON string.
   * @return A row -> json iterator.
   */
  def jsonRows: Iterator[String] = {
    val results: ResultSetIterator = new ResultSetIterator(rs)
    results.map { cursor: ResultSet =>
      JsObject(
        "rows" -> JsArray((1 to colCount).map(jsonColumnVal): _*),
        "types" -> types.toJson
      ).toJson.compactPrint
    }
  }
}
