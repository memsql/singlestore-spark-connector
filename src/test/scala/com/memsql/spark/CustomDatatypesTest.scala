package com.memsql.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types._

import scala.util.Random

class CustomDatatypesTest extends IntegrationSuiteBase {

  val dbName = "testdb"

  it("JSON columns are treated as strings by Spark") {
    executeQuery(s"""
       |create table if not exists ${dbName}.basic (
       | j JSON
       |)""".stripMargin)

    spark
      .createDF(
        List("""{"foo":"bar"}"""),
        List(("j", StringType, true))
      )
      .write
      .format("memsql")
      .mode(SaveMode.Append)
      .save("basic")

    val df = spark.read.format("memsql").load("basic")
    assertSmallDataFrameEquality(df,
                                 spark
                                   .createDF(
                                     List("""{"foo":"bar"}"""),
                                     List(("j", StringType, true))
                                   ))
  }

  it("save byte type as integer type") {
    val tableName = "bytetable"
    val byteDf = spark.createDF(
      List((4, 10: Byte)),
      List(("id", IntegerType, true), ("age", ByteType, true))
    )
    byteDf.write
      .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
      .mode(SaveMode.Append)
      .save(s"${dbName}.$tableName")
    val dataFrame = spark.read
      .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
      .load(s"${dbName}.$tableName")
    val intDF = spark.createDF(
      List((4, 10)),
      List(("id", IntegerType, true), ("age", IntegerType, true))
    )
    assertLargeDataFrameEquality(dataFrame, intDF)
  }

  describe("BinaryType") {
    // BinaryType is saved to MemSQL as BLOB
    // BLOB is loaded from MemSQL as BinaryType
    def testBinaryType(rows: List[Array[Byte]]): Unit = {
      val df = spark.createDF(
        rows.zipWithIndex,
        List(("data", BinaryType, true), ("id", IntegerType, true))
      )

      // write with LoadDataWriter
      df.write
        .format("memsql")
        .mode(SaveMode.Overwrite)
        .save("testdb.BinaryTypeLoad")

      var actualDF =
        spark.read.format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT).load("testdb.BinaryTypeLoad")
      assertLargeDataFrameEquality(actualDF, df, orderedComparison = false)

      // write with BatchInsertWriter
      df.write
        .format("memsql")
        .option("tableKey.primary", "id")
        .option("onDuplicateKeySQL", "id = id")
        .mode(SaveMode.Overwrite)
        .save("testdb.BinaryTypeInsert")

      actualDF =
        spark.read.format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT).load("testdb.BinaryTypeInsert")
      assertLargeDataFrameEquality(actualDF, df, orderedComparison = false)
    }

    it("no rows") {
      testBinaryType(List())
    }

    it("one null") {
      testBinaryType(List(null))
    }

    it("a lot of nulls") {
      testBinaryType(List(null, null, null, null, null))
    }

    it("empty row") {
      testBinaryType(List(Array()))
    }

    it("a lot of empty rows") {
      testBinaryType(List(Array(), Array(), Array(), Array()))
    }

    it("one zero byte") {
      testBinaryType(List(Array[Byte](0)))
    }

    it("negative bytes") {
      testBinaryType(List(Array[Byte](-50, -128, -1, -20)))
    }

    it("special characters") {
      testBinaryType(List(Array[Byte]('\t', '\n', '\\')))
    }

    it("row with all possible bytes") {
      testBinaryType(List(Array.range(-128, 127).map(_.toByte)))
    }

    it("a lot of special characters") {
      val specialBytes = Array[Byte](0, 127, -128, '\'', '"', '`', '\\', '/', '\t', '\n', 't', 'n',
        '\f', 'f', '[', ']', '(', ')', '@', '#', ',', '.')
      val sbLen = specialBytes.length

      def genRandomSpecialByte(): Byte = specialBytes(Random.nextInt(sbLen))

      def genRandomRow(): Array[Byte] =
        Array.fill(100)(genRandomSpecialByte())

      testBinaryType(List.fill(100)(genRandomRow()))
    }

    it("big random table") {
      def genRandomByte(): Byte = (Random.nextInt(256) - 128).toByte

      def genRandomRow(): Array[Byte] =
        Array.fill(1000)(genRandomByte())

      testBinaryType(List.fill(1000)(genRandomRow()))
    }
  }

  describe("FloatType") {
    // FloatType is saved to MemSQL as FLOAT (it is made to save storage)
    // FLOAT is loaded from MemSQL as DoubleType (it is made to simplify math operations)
    def testFloatType(options: Map[String, String], tableName: String): Unit = {
      val df = spark.createDF(
        List(10.toFloat,
             -100.4343.toFloat,
             1e8.toFloat,
             -120.toFloat,
             0.toFloat,
             5.1234.toFloat,
             -23.132123.toFloat),
        List(("id", FloatType, true))
      )

      df.write
        .format("memsql")
        .options(options)
        .save(s"testdb.$tableName")

      val actualDf =
        spark.read.format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT).load(s"testdb.$tableName")
      assertApproximateDataFrameEquality(
        actualDf,
        spark.createDF(
          List(10.0, -100.4343, 1e8, -120.0, 0.0, 5.1234, -23.132123),
          List(("id", DoubleType, true))
        ),
        precision = 0.001,
        orderedComparison = false
      )
    }

    it("LoadDataWriter") {
      testFloatType(Map.empty, "FloatTypeLoad")
    }

    it("BatchInsertWriter") {
      testFloatType(
        Map("tableKey.primary" -> "id", "onDuplicateKeySQL" -> "id = id"),
        "FloatTypeInsert"
      )
    }
  }
}
