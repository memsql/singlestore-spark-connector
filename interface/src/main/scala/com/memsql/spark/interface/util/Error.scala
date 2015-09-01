package com.memsql.spark.interface.util

class BaseException(message:String, nestedException:Exception) extends Exception(message, nestedException) {
  def this() = this("", null)
  def this(message:String) = this(message, null)
  def this(nestedException:Exception) = this(null, nestedException)
  override def toString() = message
}

object ErrorUtils {
  def getStackTraceAsString(e: Throwable): String = {
    val sb = new StringBuilder()
    getStackTraceAsString(e, sb)
  }

  private def getStackTraceAsString(e: Throwable, sb: StringBuilder): String = {
    e.synchronized {
      sb.append(e.toString)
      for (trace <- e.getStackTrace) {
        //print each trace line preceded by "    at "
        sb.append(s"\n\tat $trace")
      }

      e.getCause match {
        case null => sb.toString
        case default => getStackTraceAsString(default, sb)
      }
    }
  }
}
