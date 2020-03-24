package com.memsql.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class IssuesTest extends IntegrationSuiteBase {
  it("https://github.com/memsql/memsql-spark-connector/issues/41") {
    executeQuery("""
        | create table if not exists testdb.issue41 (
        |   start_video_pos smallint(5) unsigned DEFAULT NULL
        | )
        |""".stripMargin)

    val df = spark.createDF(
      List(1.toShort, 2.toShort, 3.toShort, 4.toShort),
      List(("start_video_pos", ShortType, true))
    )
    df.write.format("memsql").mode(SaveMode.Append).save("issue41")

    val df2 = spark.read.format("memsql").load("issue41")
    assertSmallDataFrameEquality(df2,
                                 spark.createDF(
                                   List(1, 2, 3, 4),
                                   List(("start_video_pos", IntegerType, true))
                                 ))
  }

  it("https://memsql.zendesk.com/agent/tickets/10451") {
    // parallel read should support columnar scan with filter
    executeQuery("""
      | create table if not exists testdb.ticket10451 (
      |   t text,
      |   h bigint(20) DEFAULT NULL,
      |   KEY h (h) USING CLUSTERED COLUMNSTORE
      | )
      | """.stripMargin)

    val df = spark.createDF(
      List(("hi", 2L), ("hi", 3L), ("foo", 4L)),
      List(("t", StringType, true), ("h", LongType, true))
    )
    df.write.format("memsql").mode(SaveMode.Append).save("ticket10451")

    val df2 = spark.read
      .format("memsql")
      .load("ticket10451")
      .where(col("t") === "hi")
      .where(col("h") === 3L)

    assert(df2.rdd.getNumPartitions > 1)
    assertSmallDataFrameEquality(df2,
                                 spark.createDF(
                                   List(("hi", 3L)),
                                   List(("t", StringType, true), ("h", LongType, true))
                                 ))
  }

  it("supports reading count from query") {
    val df = spark.createDF(
      List((1, "Albert"), (5, "Ronny"), (7, "Ben"), (9, "David")),
      List(("id", IntegerType, true), ("name", StringType, true))
    )
    writeTable("testdb.testcount", df)
    val data = spark.read
      .format("memsql")
      .option("query", "select count(1) from testcount where id > 1 ")
      .option("database", "testdb")
      .load()
      .collect()
    val count = data.head.getLong(0)
    assert(count == 3)
  }
}
