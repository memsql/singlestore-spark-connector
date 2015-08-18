package com.memsql.spark.etl.utils

/*
 * Inspired by https://github.com/davetron5000/shorty/blob/master/src/main/scala/shorty/Logs.scala
 */

import java.util.Properties

import org.apache.log4j._

trait Logging {
  import org.apache.log4j.Level._

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
    props.setProperty("log4j.appender.console.layout.ConversionPattern", "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")

    // Settings to quiet third party logs that are too verbose
    props.setProperty("log4j.logger.org.spark-project.jetty", "WARN")
    props.setProperty("log4j.logger.org.spark-project.jetty.util.component.AbstractLifeCycle", "ERROR")
    props.setProperty("log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper", "INFO")
    props.setProperty("log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter", "INFO")

    props
  }
}
