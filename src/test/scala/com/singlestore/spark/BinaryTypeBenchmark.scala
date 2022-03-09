package com.singlestore.spark

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.singlestore.spark.BatchInsertBenchmark.{df, executeQuery}
import org.apache.spark.sql.types.{BinaryType, IntegerType}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

// BinaryTypeBenchmark is written to writing of the BinaryType with CPU profiler
// this feature is accessible in Ultimate version of IntelliJ IDEA
// see https://www.jetbrains.com/help/idea/async-profiler.html#profile for more details
object BinaryTypeBenchmark extends App {
  final val clusterHost: String = sys.props.getOrElse("singlestore.host", "localhost")
  final val clusterPort: String = sys.props.getOrElse("singlestore.port", "5508")
  final val adminPort: String   = sys.props.getOrElse("singlestore.port", "5506")

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.datasource.singlestore.clusterEndpoints", s"${clusterHost}:${clusterPort}")
    .config("spark.datasource.singlestore.adminEndpoint", s"${clusterHost}:${adminPort}")
    .config("spark.datasource.singlestore.database", "testdb")
    .getOrCreate()

  def jdbcConnection: Loan[Connection] = {
    val connProperties = new Properties()
    connProperties.put("user", "root")

    Loan(
      DriverManager.getConnection(
        s"jdbc:singlestore://$clusterHost:$adminPort",
        connProperties
      ))
  }

  def executeQuery(sql: String): Unit = {
    jdbcConnection.to(conn => Loan(conn.createStatement).to(_.execute(sql)))
  }

  executeQuery("set global default_partitions_per_leaf = 2")
  executeQuery("drop database if exists testdb")
  executeQuery("create database testdb")

  def genRandomByte(): Byte = (Random.nextInt(256) - 128).toByte
  def genRandomRow(): Array[Byte] =
    Array.fill(1000)(genRandomByte())

  val df = spark.createDF(
    List.fill(100000)(genRandomRow()).zipWithIndex,
    List(("data", BinaryType, true), ("id", IntegerType, true))
  )

  val start1 = System.nanoTime()
  df.write
    .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
    .mode(SaveMode.Overwrite)
    .save("testdb.LoadData")

  println("Elapsed time: " + (System.nanoTime() - start1) + "ns [LoadData CSV]")

  val start2 = System.nanoTime()
  df.write
    .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
    .option("tableKey.primary", "id")
    .option("onDuplicateKeySQL", "data = data")
    .mode(SaveMode.Overwrite)
    .save("testdb.BatchInsert")

  println("Elapsed time: " + (System.nanoTime() - start2) + "ns [BatchInsert]")

  val avroStart = System.nanoTime()
  df.write
    .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
    .mode(SaveMode.Overwrite)
    .option(SinglestoreOptions.LOAD_DATA_FORMAT, "Avro")
    .save("testdb.AvroSerialization")
  println("Elapsed time: " + (System.nanoTime() - avroStart) + "ns [LoadData Avro] ")
}
