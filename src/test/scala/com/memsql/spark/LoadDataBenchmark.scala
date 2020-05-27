package com.memsql.spark

import java.sql.{Connection, Date, DriverManager}
import java.time.{Instant, LocalDate}
import java.util.Properties

import org.apache.spark.sql.types._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

// LoadDataBenchmark is written to test load data with CPU profiler
// this feature is accessible in Ultimate version of IntelliJ IDEA
// see https://www.jetbrains.com/help/idea/async-profiler.html#profile for more details
object LoadDataBenchmark extends App {
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

  def genRow(): (Long, Int, Double, String) =
    (Random.nextLong(), Random.nextInt(), Random.nextDouble(), Random.nextString(20))
  val df =
    spark.createDF(
      List.fill(1000000)(genRow()),
      List(("LongType", LongType, true),
           ("IntType", IntegerType, true),
           ("DoubleType", DoubleType, true),
           ("StringType", StringType, true))
    )

  val start = System.nanoTime()
  df.write
    .format("memsql")
    .mode(SaveMode.Append)
    .save("testdb.batchinsert")

  val diff = System.nanoTime() - start
  println("Elapsed time: " + diff + "ns [CSV serialization] ")

  executeQuery("truncate testdb.batchinsert")

  val avroStart = System.nanoTime()
  df.write
    .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
    .mode(SaveMode.Append)
    .option(MemsqlOptions.LOAD_DATA_FORMAT, "Avro")
    .save("testdb.batchinsert")
  val avroDiff = System.nanoTime() - avroStart
  println("Elapsed time: " + avroDiff + "ns [Avro serialization] ")
}
