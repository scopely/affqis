package com.scopely.affqis

import java.sql.Connection

/**
 * A trait for holding database info. You can
 */
trait DBConnection {
  def connection: Connection
}
