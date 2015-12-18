// scalastyle:off regex magic.number

package com.memsql.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.memsql.MemSQLContext
import org.apache.spark.sql.memsql.SparkImplicits.DataFrameFunctions
import org.apache.spark.sql.memsql.test.TestUtils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object TestSaveToMemSQLNoNodesAvailable {
  def main(args: Array[String]): Unit = new TestSaveToMemSQLNoNodesAvailable
}

class TestSaveToMemSQLNoNodesAvailable extends TestApp {
  def runTest(sc: SparkContext, msc: MemSQLContext): Unit = {
    val rdd = sc.parallelize(
      Array(Row(1, "pieguy"),
            Row(2, "gbop"),
            Row(3, "berry\ndave"),
            Row(4, "psy\tduck")))

    val schema = StructType(
      Array(
        StructField("a", IntegerType, true),
        StructField("b", StringType, true)))

    val df = msc.createDataFrame(rdd, schema)
    df.saveToMemSQL("t")

    TestUtils.detachPartitions(this)

    try {
      df.saveToMemSQL("t")
      assert(false)
    } catch {
      case e: SaveToMemSQLException => { }
      case _ => assert(false)
    }
  }
}
