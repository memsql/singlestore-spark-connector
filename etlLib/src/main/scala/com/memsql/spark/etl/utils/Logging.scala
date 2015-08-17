package com.memsql.spark.etl.utils

/*
 * Inspired by https://github.com/davetron5000/shorty/blob/master/src/main/scala/shorty/Logs.scala
 */

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
