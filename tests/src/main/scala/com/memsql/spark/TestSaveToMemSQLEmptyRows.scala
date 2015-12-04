// scalastyle:off regex
package com.memsql.spark

import org.apache.spark.sql.memsql.SparkImplicits._
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.memsql.MemSQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TestSaveToMemSQLEmptyRows {
  def main(args: Array[String]): Unit = new TestSaveToMemSQLEmptyRows
}

class TestSaveToMemSQLEmptyRows extends TestBase with Logging {
  def runTest(sc: SparkContext, msc: MemSQLContext): Unit = {
    val rdd = sc.parallelize(Array(Row()))
    val schema = StructType(Array[StructField]())
    val df = msc.createDataFrame(rdd, schema)
    df.saveToMemSQL(dbName, "empty_rows_table")

    val schema1 = StructType(
      Array(
        StructField("memsql_insert_time", TimestampType, true)
      )
    )
    val df1 = msc.table("empty_rows_table")
    assert(df1.schema.equals(schema1))
    assert(df1.count == 1)
  }
}
