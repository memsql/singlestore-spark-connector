// scalastyle:off regex magic.number

package com.memsql.spark.connector.sql

import com.memsql.spark.connector._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec


class TestCreateTableDefaultColumnsSpec extends FlatSpec with SharedMemSQLContext{
  "TestCreateTableDefaultColumnsSpec" should "test default columns" in {
    val rdd = sc.parallelize(Array(Row(1)))
    val schema = StructType(
      Array(
        StructField("a", IntegerType, true),
        StructField("b", StringType, true),
        StructField("c", StringType, false),
        StructField("d", BinaryType, true),
        StructField("e", BinaryType, false)))
    val emptyDf = ss.createDataFrame(sc.parallelize(Array[Row]()), schema)
    emptyDf.saveToMemSQL(dbName, "default_columns_table")
    val df = ss.createDataFrame(rdd, StructType(Array(StructField("a", IntegerType, true))))
    df.saveToMemSQL(dbName, "default_columns_table")

    val df1 = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map("path" -> (dbName + ".default_columns_table")))
      .load()

    assert(df1.count == 1)
    assert(df1.collect()(0).getAs[Int](0) == 1)
    assert(df1.collect()(0).isNullAt(1))
    assert(df1.collect()(0).getAs[String](2) == "")
    assert(df1.collect()(0).isNullAt(3))
    assert(df1.collect()(0).getAs[Array[Byte]](4).length == 0)
  }
}
