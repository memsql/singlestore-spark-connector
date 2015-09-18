package com.memsql.spark.etl

import java.io.{InputStream, InputStreamReader, BufferedReader}

object Meta {
  def versionFileName: String = "MemSQLETLVersion"

  def version(): String = {
    val resourceStream = getClass.getResourceAsStream(s"/$versionFileName")
    readVersionFromResource(resourceStream)
  }

  def readVersionFromResource(stream: InputStream): String = {
    val reader = new BufferedReader(new InputStreamReader(stream))
    reader.readLine
  }

  def main(args: Array[String]): Unit = {
    println(version)
  }
}
