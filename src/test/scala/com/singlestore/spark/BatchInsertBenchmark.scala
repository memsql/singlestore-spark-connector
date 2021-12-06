package com.singlestore.spark

import java.sql.{Connection, Date, DriverManager}
import java.time.LocalDate
import java.util.Properties

import org.apache.spark.sql.types._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

// BatchInsertBenchmark is written to test batch insert with CPU profiler
// this feature is accessible in Ultimate version of IntelliJ IDEA
// see https://www.jetbrains.com/help/idea/async-profiler.html#profile for more details
object BatchInsertBenchmark extends App {
  final val masterHost: String = sys.props.getOrElse("singlestore.host", "localhost")
  final val masterPort: String = sys.props.getOrElse("singlestore.port", "5506")

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.datasource.singlestore.ddlEndpoint", s"${masterHost}:${masterPort}")
    .config("spark.datasource.singlestore.database", "testdb")
    .getOrCreate()

  def jdbcConnection: Loan[Connection] = {
    val connProperties = new Properties()
    connProperties.put("user", "root")

    Loan(
      DriverManager.getConnection(
        s"jdbc:singlestore://$masterHost:$masterPort",
        connProperties
      ))
  }

  def executeQuery(sql: String): Unit = {
    jdbcConnection.to(conn => Loan(conn.createStatement).to(_.execute(sql)))
  }

  executeQuery("set global default_partitions_per_leaf = 2")
  executeQuery("drop database if exists testdb")
  executeQuery("create database testdb")

  def genDate() =
    Date.valueOf(LocalDate.ofEpochDay(LocalDate.of(2001, 4, 11).toEpochDay + Random.nextInt(10000)))
  def genRow(): (Long, Int, Double, String, Date) =
    (Random.nextLong(), Random.nextInt(), Random.nextDouble(), Random.nextString(20), genDate())
  val df =
    spark.createDF(
      List.fill(1000000)(genRow()),
      List(("LongType", LongType, true),
           ("IntType", IntegerType, true),
           ("DoubleType", DoubleType, true),
           ("StringType", StringType, true),
           ("DateType", DateType, true))
    )

  val start = System.nanoTime()
  df.write
    .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
    .option("tableKey.primary", "IntType")
    .option("onDuplicateKeySQL", "IntType = IntType")
    .mode(SaveMode.Append)
    .save("testdb.batchinsert")

  val diff = System.nanoTime() - start
  println("Elapsed time: " + diff + "ns")
}
