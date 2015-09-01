package com.memsql.spark.interface.util

import java.io.{PrintWriter, StringWriter}

class BaseException(message:String, nestedException:Exception) extends Exception(message, nestedException) {
  def this() = this("", null)
  def this(message:String) = this(message, null)
  def this(nestedException:Exception) = this(null, nestedException)
  override def toString() = message
}

object ErrorUtils {
  def getStackTraceAsString(e: Throwable): String = {
    val sw = new StringWriter()
    e.printStackTrace(new PrintWriter(sw))
    sw.toString
  }
}
