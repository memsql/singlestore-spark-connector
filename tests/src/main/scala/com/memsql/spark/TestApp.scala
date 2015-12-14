// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark

import org.apache.spark.sql.memsql.MemSQLContext
import org.apache.spark.sql.memsql.test.TestBase
import org.apache.spark.SparkContext

abstract class TestApp extends TestBase {

  // Override this method to implement a test.
  def runTest(sc: SparkContext, msc: MemSQLContext): Unit

  sparkUp(local = false)
  runTest(sc, msc)
}
