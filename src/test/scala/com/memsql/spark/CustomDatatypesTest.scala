package com.memsql.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._

class CustomDatatypesTest extends IntegrationSuiteBase {

  val dbName = "testdb"

  it("JSON columns are treated as strings by Spark") {
    executeQuery(s"""
       |create table if not exists ${dbName}.basic (
       | j JSON
       |)""".stripMargin)

    spark
      .createDF(
        List("""{"foo":"bar"}"""),
        List(("j", StringType, true))
      )
      .write
      .format("memsql")
      .mode(SaveMode.Append)
      .save("basic")

    val df = spark.read.format("memsql").load("basic")
    assertSmallDataFrameEquality(df,
                                 spark
                                   .createDF(
                                     List("""{"foo":"bar"}"""),
                                     List(("j", StringType, true))
                                   ))
  }

  it("save byte type as integer type") {
    val tableName = "bytetable"
    val byteDf = spark.createDF(
      List((4, 10: Byte)),
      List(("id", IntegerType, true), ("age", ByteType, true))
    )
    byteDf.write
      .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
      .mode(SaveMode.Append)
      .save(s"${dbName}.$tableName")
    val dataFrame = spark.read
      .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
      .load(s"${dbName}.$tableName")
    val intDF = spark.createDF(
      List((4, 10)),
      List(("id", IntegerType, true), ("age", IntegerType, true))
    )
    assertLargeDataFrameEquality(dataFrame, intDF)
  }
}
