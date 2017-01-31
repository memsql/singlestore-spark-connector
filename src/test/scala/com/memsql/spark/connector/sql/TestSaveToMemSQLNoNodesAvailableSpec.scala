// scalastyle:off regex magic.number

package com.memsql.spark.connector.sql

import com.memsql.spark.SaveToMemSQLException
import com.memsql.spark.connector._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FlatSpec


class TestSaveToMemSQLNoNodesAvailableSpec extends FlatSpec with SharedMemSQLContext{
  "TestSaveToMemSQLNoNodesAvailableSpec" should "blah" in {
    val rdd = sc.parallelize(
      Array(Row(1, "pieguy"),
        Row(2, "gbop"),
        Row(3, "berry\ndave"),
        Row(4, "psy\tduck")))

    val schema = StructType(
      Array(
        StructField("a", IntegerType, true),
        StructField("b", StringType, true)))

    val df = ss.createDataFrame(rdd, schema)
    df.saveToMemSQL("t")

    TestUtils.detachPartitions(this)

    try {
      df.saveToMemSQL("t")
      assert(false)
    } catch {
      case e: SaveToMemSQLException => { }
      case _: Throwable => assert(false)
    }
  }
}
