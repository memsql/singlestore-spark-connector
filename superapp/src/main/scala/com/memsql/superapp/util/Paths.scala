package com.memsql.superapp.util

import java.io.File

class PathException(message:String, nestedException:Exception) extends BaseException(message, nestedException) {
  def this() = this("", null)
  def this(message:String) = this(message, null)
  def this(nestedException:Exception) = this(null, nestedException)
}

object Paths {
  var BASE_DIR:String = null
  var JAR_DIR:String = null

  def initialize(base:String): Unit = {
    val baseDir = new File(base)
    val jarDir = new File(base, "jars")
    try {
      baseDir.mkdir
      jarDir.mkdir
      if (!baseDir.isDirectory || !jarDir.isDirectory) {
        throw new PathException(s"Could not create directories for $base")
      }
    } catch {
      case p: PathException => throw p
      case e: Exception => throw new PathException(s"Could not create directories for $base", e)
    }

    BASE_DIR = baseDir.toString
    JAR_DIR = jarDir.toString
  }

  def join(a:String, b:String): String = {
    new File(a, b).toString
  }

  def exists(path:String): Boolean = {
    new File(path).exists
  }
}
