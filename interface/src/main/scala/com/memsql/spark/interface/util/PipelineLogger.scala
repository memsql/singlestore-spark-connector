package com.memsql.spark.interface.util

import java.io.{StringWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable.Queue

class PipelineLogger(override val name: String, val keepEntries: Boolean=true) extends PhaseLogger {
  override protected val logger: Logger = Logger.getRootLogger

  private val dateFormatter = new SimpleDateFormat("yy/MM/dd HH:mm:ss")
  private val logEntries: Queue[String] = new Queue[String]()

  override private[memsql] def log(level: Level, message: => String): Unit = {
    super.log(level, message)
    if (keepEntries) {
      enqueueMessage(level, message)
    }
  }

  override private[memsql] def log(level: Level, message: => String, ex: Throwable): Unit = {
    super.log(level, message, ex)
    if (keepEntries) {
      enqueueMessage(level, message, ex)
    }
  }

  // Used by the Python logging implementation.
  def Py4JLog(level: String, message: String): Unit = {
    level match {
      case "DEBUG" => log(Level.DEBUG, message)
      case "INFO" => log(Level.INFO, message)
      case "WARN" => log(Level.WARN, message)
      case "ERROR" => log(Level.ERROR, message)
      case "FATAL" => log(Level.FATAL, message)
    }
  }

  def getLogEntries(): List[String] = logEntries.toList
  def clearLogEntries(): Unit = logEntries.clear

  private def enqueueMessage(level: Level, message: String, ex: Throwable = null): Unit = {
    if (logLevelAllowed(level)) {
      val buf = new StringBuffer()

      // This is the same logging layout we use in
      // com.memsql.spark.etl.utils.Logging, but without newlines at the end and
      // without the logger name.
      buf.append(dateFormatter.format(new Date()))
      buf.append(" ").append(level).append(": ").append(message)

      if (ex != null) {
        val sw = new StringWriter()
        val exBuf = new PrintWriter(sw)
        ex.printStackTrace(exBuf)
        exBuf.flush()
        buf.append("\n").append(sw.toString)
      }

      logEntries.enqueue(buf.toString)
    }
  }
}
