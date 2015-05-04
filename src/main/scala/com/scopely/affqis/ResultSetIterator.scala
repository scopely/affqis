package com.scopely.affqis

import java.sql.ResultSet

/**
 * A ResultSet wrapper that provides an iterable interface.
 * @param rs A normal set of results from a SQL call.
 */
class ResultSetIterator(rs: ResultSet) extends Iterator[ResultSet] {
  def hasNext: Boolean = rs.next()
  def next(): ResultSet = rs
}
