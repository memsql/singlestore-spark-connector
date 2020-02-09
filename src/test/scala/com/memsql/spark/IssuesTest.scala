package com.memsql.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll

class IssuesTest extends IntegrationSuiteBase with BeforeAndAfterAll {
  override def beforeAll() = {
    super.beforeAll()
    executeQuery("drop database if exists issues")
    executeQuery("create database issues")
  }

  it("https://github.com/memsql/memsql-spark-connector/issues/41") {
    executeQuery("""
        | create table issues.issue41 (
        |   start_video_pos smallint(5) unsigned DEFAULT NULL
        | )
        |""".stripMargin)

    val df = spark.createDF(
      List((1.toShort), (2.toShort), (3.toShort), (4.toShort)),
      List(("start_video_pos", ShortType, true))
    )
    df.write.format("memsql").mode(SaveMode.Append).save("issues.issue41")

    val df2 = spark.read.format("memsql").load("issues.issue41")
    assertSmallDataFrameEquality(df2,
                                 spark.createDF(
                                   List((1), (2), (3), (4)),
                                   List(("start_video_pos", IntegerType, true))
                                 ))
  }
}
