package com.memsql.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class CustomDatatypesTest
    extends IntegrationSuiteBase
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  override def beforeAll() = {
    super.beforeAll()
    executeQuery("drop database if exists customdatatypes")
    executeQuery("create database customdatatypes")

  }

  override def beforeEach() = {
    super.beforeEach()
    executeQuery("""
                   |create table customdatatypes.basic (
                   | j JSON
                   |)""".stripMargin)
  }

  override def afterEach() = {
    super.afterEach()
    executeQuery("drop table customdatatypes.basic")
  }

  it("JSON columns are treated as strings by Spark") {
    spark
      .createDF(
        List("""{"foo":"bar"}"""),
        List(("j", StringType, true))
      )
      .write
      .format("memsql")
      .mode(SaveMode.Append)
      .save("customdatatypes.basic")

    val df = spark.read.format("memsql").load("customdatatypes.basic")
    assertSmallDataFrameEquality(df,
                                 spark
                                   .createDF(
                                     List("""{"foo":"bar"}"""),
                                     List(("j", StringType, true))
                                   ))
  }
}
