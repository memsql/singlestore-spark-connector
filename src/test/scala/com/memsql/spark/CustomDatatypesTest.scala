package com.memsql.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class CustomDatatypesTest extends IntegrationSuiteBase {

  it("JSON columns are treated as strings by Spark") {
    executeQuery("""
       |create table testdb.basic (
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
}
