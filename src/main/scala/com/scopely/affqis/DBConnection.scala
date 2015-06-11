package com.scopely.affqis

import java.sql.{DriverManager, Connection}
import java.util.{UUID, TimerTask, Timer}

import org.slf4j.{Logger, LoggerFactory}

/**
 * A trait for holding database info. You can
 */
trait DBConnection {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  def connection: Connection

  val id: UUID = UUID.randomUUID()
  val janitor: Timer = new Timer()

  private var janitorTask: Option[TimerTask] = None

  /**
   * Close a JDBC connection. Override to change how connections get closed.
   */
  def onDisconnect(): Unit = {
    log.info(s"Closing connection for id ${id.toString}")
    janitor.cancel()
    connection.close()
  }

  def scheduleDisconnect(afterTimeout: => Unit): Unit =
    scheduleDisconnect(1000 * 60 * 30)(afterTimeout)

  def scheduleDisconnect(timeout: Int)(afterTimeout: => Unit): Unit = {
    janitorTask foreach {_.cancel()}
    val task = new TimerTask {
      override def run(): Unit = {
        log.info(s"Killing connection $id due to idle timeout...")
        onDisconnect()
        afterTimeout
      }
    }

    janitorTask = Some(task)
    janitor.schedule(task, timeout)
  }
}

object DBConnection {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Connect via JDBC to a database.
   */
  def connect(uri: String): Connection = {
    log.info(s"Establishing a connection to $uri")
    DriverManager.getConnection(uri)
  }

  def connect(uri: String, user: String, pass: String = ""): Connection = {
    log.info(s"Establishing a connection to $uri")
    DriverManager.getConnection(uri, user, pass)
  }
}