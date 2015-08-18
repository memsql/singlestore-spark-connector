package com.memsql.spark.interface.util

class BaseException(message:String, nestedException:Exception) extends Exception(message, nestedException) {
  def this() = this("", null)
  def this(message:String) = this(message, null)
  def this(nestedException:Exception) = this(null, nestedException)
  override def toString() = message
}
