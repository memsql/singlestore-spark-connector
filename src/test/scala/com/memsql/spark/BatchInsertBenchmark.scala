package com.memsql.spark

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{SaveMode, SparkSession}

// BatchInsertBenchmark is written to test batch insert with CPU profiler
// this feature is accessible in Ultimate version of IntelliJ IDEA
// see https://www.jetbrains.com/help/idea/async-profiler.html#profile for more details
object BatchInsertBenchmark extends App {
  final val masterHost: String = sys.props.getOrElse("memsql.host", "localhost")
  final val masterPort: String = sys.props.getOrElse("memsql.port", "5506")

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.datasource.memsql.ddlEndpoint", s"${masterHost}:${masterPort}")
    .config("spark.datasource.memsql.database", "testdb")
    .getOrCreate()

  def jdbcConnection: Loan[Connection] = {
    val connProperties = new Properties()
    connProperties.put("user", "root")

    Loan(
      DriverManager.getConnection(
        s"jdbc:mysql://$masterHost:$masterPort",
        connProperties
      ))
  }

  def executeQuery(sql: String): Unit = {
    jdbcConnection.to(conn => Loan(conn.createStatement).to(_.execute(sql)))
  }

  executeQuery("set global default_partitions_per_leaf = 2")
  executeQuery("drop database if exists testdb")
  executeQuery("create database testdb")

  val df = spark.range(5000000)
  df.write
    .format("memsql")
    .option("tableKey.primary", "id")
    .option("onDuplicateKeySQL", "id = id + 1")
    .mode(SaveMode.Append)
    .save("testdb.batchinsert")
}
