package com.memsql.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.memsql.spark.MemsqlOptions.CompressionType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.BeforeAndAfterEach

class SanityTest extends IntegrationSuiteBase with BeforeAndAfterEach {
  var df: DataFrame = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    df = spark.createDF(
      List((1, "Albert"), (5, "Ronny"), (7, "Ben"), (9, "David")),
      List(("id", IntegerType, true), ("name", StringType, true))
    )
    executeQuery("create database if not exists test")
    writeTable("test.foo", df)
  }

  it("sets strict sql session variables") {
    // set a large but not exactly the same sql select limit
    executeQuery("set global sql_select_limit = 18446744000000000000")

    val variables = spark.read
      .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
      .option(MemsqlOptions.TABLE_NAME, "information_schema.session_variables")
      .load()
      .collect()
      .groupBy(r => r.get(0))
      .mapValues(r => r.map(_.getString(1)).head)

    assert(variables("COLLATION_SERVER") == "utf8_general_ci")
    assert(variables("SQL_SELECT_LIMIT") == "18446744073709551615")
    assert(variables("COMPILE_ONLY") == "OFF")

    val sql_mode = spark.read
      .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
      .option(MemsqlOptions.QUERY, "select @@sql_mode")
      .load()
      .collect()
      .head
      .getString(0)
    assert(sql_mode == "ONLY_FULL_GROUP_BY,STRICT_ALL_TABLES")
  }

  it("DataSource V1 read sanity") {
    val x = spark.read
      .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
      .option(MemsqlOptions.TABLE_NAME, "test.foo")
      .load()
      .withColumn("hello", lit(2))
      .filter(col("id") > 1)
      .limit(1000)
      .groupBy(col("id"))
      .agg(count("*"))

    assertSmallDataFrameEquality(
      x,
      df.withColumn("hello", lit(2))
        .filter(col("id") > 1)
        .limit(1000)
        .groupBy(col("id"))
        .agg(count("*")),
      orderedComparison = false
    )
  }

  it("DataSource V1 write sanity") {
    for (compression <- CompressionType.values) {
      for (truncate <- Seq(false, true)) {
        println(
          s"testing datasource with compression=$compression, truncate=$truncate"
        )
        df.write
          .format(DefaultSource.MEMSQL_SOURCE_NAME)
          .option(MemsqlOptions.TABLE_NAME, "test.tb2")
          .option(MemsqlOptions.LOAD_DATA_COMPRESSION, compression.toString)
          .option(MemsqlOptions.TRUNCATE, truncate.toString)
          .mode(SaveMode.Overwrite)
          .save()

        val x = spark.read
          .format("jdbc")
          .option("url", s"jdbc:mysql://$masterHost:$masterPort/test")
          .option("dbtable", "test.tb2")
          .option("user", "root")
          .load()

        assertSmallDataFrameEquality(x, df, true, true)
      }
    }
  }
}
