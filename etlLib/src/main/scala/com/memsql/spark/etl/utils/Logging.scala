package com.memsql.spark.etl.utils

import java.util.Properties

import org.apache.log4j._
import org.apache.log4j.Level._

/*
 * Logger interface used for user defined Extractors, Transformers, and Loaders
 */
abstract class PhaseLogger {
  protected val logger: Logger
  protected val name: String
  private var level: Level = DEBUG

  def debug(message: => String) = log(DEBUG, message)
  def debug(message: => String, ex:Throwable) = log(DEBUG, message, ex)

  def info(message: => String) = log(INFO, message)
  def info(message: => String, ex:Throwable) = log(INFO, message, ex)

  def warn(message: => String) = log(WARN, message)
  def warn(message: => String, ex:Throwable) = log(WARN, message, ex)

  def error(message: => String) = log(ERROR, message)
  def error(message: => String, ex:Throwable) = log(ERROR, message, ex)

  def fatal(message: => String) = log(FATAL, message)
  def fatal(message: => String, ex:Throwable) = log(FATAL, message, ex)

  def setLevel(level: Level) = {
    this.level = level
  }

  private val logLevels: List[Level] = List(DEBUG, INFO, WARN, ERROR, FATAL)
  def logLevelAllowed(level: Level): Boolean = {
    logLevels.indexOf(level) >= logLevels.indexOf(this.level)
  }

  private[memsql] def log(level: Level, msg: => String): Unit = {
    if (!logLevelAllowed(level)) return

    val message = s"$name: $msg"
    level match {
      case DEBUG => if (logger.isEnabledFor(DEBUG)) logger.debug(message)
      case INFO => if (logger.isEnabledFor(INFO)) logger.info(message)
      case WARN => if (logger.isEnabledFor(WARN)) logger.warn(message)
      case ERROR => if (logger.isEnabledFor(ERROR)) logger.error(message)
      case FATAL => if (logger.isEnabledFor(FATAL)) logger.fatal(message)
    }
  }

  private[memsql] def log(level: Level, msg: => String, ex:Throwable): Unit = {
    if (!logLevelAllowed(level)) return

    val message = s"$name: $msg"
    level match {
      case DEBUG => if (logger.isEnabledFor(DEBUG)) logger.debug(message, ex)
      case INFO => if (logger.isEnabledFor(INFO)) logger.info(message, ex)
      case WARN => if (logger.isEnabledFor(WARN)) logger.warn(message, ex)
      case ERROR => if (logger.isEnabledFor(ERROR)) logger.error(message, ex)
      case FATAL => if (logger.isEnabledFor(FATAL)) logger.fatal(message, ex)
    }
  }
}

/*
 * Inspired by https://github.com/davetron5000/shorty/blob/master/src/main/scala/shorty/Logs.scala
 */

trait Logging {
  private val logger = Logger.getLogger(getClass.getName)

  def logDebug(message: => String) = if (logger.isEnabledFor(DEBUG)) logger.debug(message)
  def logDebug(message: => String, ex:Throwable) = if (logger.isEnabledFor(DEBUG)) logger.debug(message, ex)

  def logInfo(message: => String) = if (logger.isEnabledFor(INFO)) logger.info(message)
  def logInfo(message: => String, ex:Throwable) = if (logger.isEnabledFor(INFO)) logger.info(message, ex)

  def logWarn(message: => String) = if (logger.isEnabledFor(WARN)) logger.warn(message)
  def logWarn(message: => String, ex:Throwable) = if (logger.isEnabledFor(WARN)) logger.warn(message, ex)

  def logError(message: => String) = if (logger.isEnabledFor(ERROR)) logger.error(message)
  def logError(message: => String, ex:Throwable) = if (logger.isEnabledFor(ERROR)) logger.error(message, ex)

  def logFatal(message: => String) = if (logger.isEnabledFor(FATAL)) logger.fatal(message)
  def logFatal(message: => String, ex:Throwable) = if (logger.isEnabledFor(FATAL)) logger.fatal(message, ex)
}

object Logging {
  def defaultProps: Properties = {
    //Use Spark defaults for logging
    val props = new Properties()

    // Set everything to be logged to the console
    props.setProperty("log4j.rootCategory", "INFO, console")
    props.setProperty("log4j.appender.console", "org.apache.log4j.ConsoleAppender")
    props.setProperty("log4j.appender.console.target", "System.err")
    props.setProperty("log4j.appender.console.layout", "org.apache.log4j.PatternLayout")
    props.setProperty("log4j.appender.console.layout.ConversionPattern", "%d{yy/MM/dd HH:mm:ss} %p %m%n")

    // Settings to quiet third party logs that are too verbose
    props.setProperty("log4j.logger.org.spark-project.jetty", "WARN")
    props.setProperty("log4j.logger.org.spark-project.jetty.util.component.AbstractLifeCycle", "ERROR")
    props.setProperty("log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper", "INFO")
    props.setProperty("log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter", "INFO")

    props
  }
}
