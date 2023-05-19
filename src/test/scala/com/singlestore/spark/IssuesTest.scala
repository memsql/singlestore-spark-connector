package com.singlestore.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import java.util.logging.Logger
import org.slf4j.LoggerFactory
import org.apache.log4j.LogManager
import org.apache.log4j.Level

class IssuesTest extends IntegrationSuiteBase {
  it("test") {
    val df = spark.read.format("singlestore")
    .option("readAheadInputRateLimit", "12345")
    .option("readFetchSize", "10000")
    .load("db.t")
    log.error("AAAAA " + df.cache().count())
  }
}
