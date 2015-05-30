package com.scopely.affqis

import java.io.{PrintWriter, StringWriter}

/**
 * Utilities for dealing with Java exceptions.
 */
object ExceptionUtils {

  /**
   * Get an exception's stacktrace as a string.
   */
  def getTrace(exn: Exception): String = {
    val traceStringWriter: StringWriter = new StringWriter()
    val traceWriter: PrintWriter = new PrintWriter(traceStringWriter, true)
    exn.printStackTrace(traceWriter)
    traceStringWriter.getBuffer.toString
  }
}
