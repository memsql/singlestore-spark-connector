// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark

import org.apache.spark.sql.memsql.MemSQLContext
import org.apache.spark.{SparkContext, Logging}

/**
  * This file is just a test template - copy it to write a test.
  */
object TestTemplate {
  def main(args: Array[String]): Unit = new TestTemplate
}

class TestTemplate extends TestApp with Logging {
  def runTest(sc: SparkContext, msc: MemSQLContext): Unit = {
  }
}

