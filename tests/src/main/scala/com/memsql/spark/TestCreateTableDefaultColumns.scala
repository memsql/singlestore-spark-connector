// scalastyle:off regex magic.number
package com.memsql.spark

import com.memsql.spark.connector._
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.memsql.MemSQLContext
import org.apache.spark.sql.types._

object TestCreateTableDefaultColumns {
  def main(args: Array[String]): Unit = new TestCreateTableDefaultColumns
}

class TestCreateTableDefaultColumns extends TestApp with Logging {
  def runTest(sc: SparkContext, msc: MemSQLContext): Unit = {
    val rdd = sc.parallelize(Array(Row(1)))
    val schema = StructType(
      Array(
        StructField("a", IntegerType, true),
        StructField("b", StringType, true),
        StructField("c", StringType, false),
        StructField("d", BinaryType, true),
        StructField("e", BinaryType, false)))
    val emptyDf = msc.createDataFrame(sc.parallelize(Array[Row]()), schema)
    emptyDf.saveToMemSQL(dbName, "default_columns_table")
    val df = msc.createDataFrame(rdd, StructType(Array(StructField("a", IntegerType, true))))
    df.saveToMemSQL(dbName, "default_columns_table")

    val df1 = msc.table("default_columns_table")
    assert(df1.count == 1)
    assert(df1.collect()(0).getAs[Int](0) == 1)
    assert(df1.collect()(0).isNullAt(1))
    assert(df1.collect()(0).getAs[String](2) == "")
    assert(df1.collect()(0).isNullAt(3))
    assert(df1.collect()(0).getAs[Array[Byte]](4).length == 0)
  }
}
