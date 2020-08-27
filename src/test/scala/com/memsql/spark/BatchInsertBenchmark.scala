package com.memsql.spark

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
  final val masterHost: String = sys.props.getOrElse("memsql.host", "localhost")
  final val masterPort: String = sys.props.getOrElse("memsql.port", "5506")

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.datasource.memsql.ddlEndpoint", s"${masterHost}:${masterPort}")
    .config("spark.datasource.memsql.database", "testdb")
    .config("spark.sql.catalog.spark_catalog", "com.memsql.spark.v2.MemsqlTableCatalog")
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

  println("Data: ")
  df.show(10)

  def writeWithSource(sourceName: String): Unit = {
    val start = System.nanoTime()
    df.write
      .format(sourceName)
      .option("tableKey.primary", "IntType")
      .option("onDuplicateKeySQL", "IntType = IntType")
      .mode(SaveMode.ErrorIfExists)
      .saveAsTable("testdb.batchinsert")
    val diff = System.nanoTime() - start
    println("Elapsed time: " + diff + "ns")
  }

  println("Data Source V1")
  writeWithSource("memsql")

  executeQuery("DROP TABLE testdb.batchinsert")

  println("Data Source V2")
  writeWithSource("com.memsql.spark.v2")
}
