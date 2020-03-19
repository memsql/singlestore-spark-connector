package com.memsql.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll

class IssuesTest extends IntegrationSuiteBase with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
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
      List(1.toShort, 2.toShort, 3.toShort, 4.toShort),
      List(("start_video_pos", ShortType, true))
    )
    df.write.format("memsql").mode(SaveMode.Append).save("issues.issue41")

    val df2 = spark.read.format("memsql").load("issues.issue41")
    assertSmallDataFrameEquality(df2,
                                 spark.createDF(
                                   List(1, 2, 3, 4),
                                   List(("start_video_pos", IntegerType, true))
                                 ))
  }

  it("https://memsql.zendesk.com/agent/tickets/10451") {
    // parallel read should support columnar scan with filter
    executeQuery("""
      | create table issues.ticket10451 (
      |   t text,
      |   h bigint(20) DEFAULT NULL,
      |   KEY h (h) USING CLUSTERED COLUMNSTORE
      | )
      | """.stripMargin)

    val df = spark.createDF(
      List(("hi", 2L), ("hi", 3L), ("foo", 4L)),
      List(("t", StringType, true), ("h", LongType, true))
    )
    df.write.format("memsql").mode(SaveMode.Append).save("issues.ticket10451")

    val df2 = spark.read
      .format("memsql")
      .load("issues.ticket10451")
      .where(col("t") === "hi")
      .where(col("h") === 3L)

    assert(df2.rdd.getNumPartitions > 1)
    assertSmallDataFrameEquality(df2,
                                 spark.createDF(
                                   List(("hi", 3L)),
                                   List(("t", StringType, true), ("h", LongType, true))
                                 ))
  }
}
