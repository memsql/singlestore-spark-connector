package com.memsql.spark

import java.sql.Timestamp

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types.{IntegerType, NullType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.util.Try

class LoadDataTest extends IntegrationSuiteBase with BeforeAndAfterEach with BeforeAndAfterAll {
  var df: DataFrame = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    df = spark.createDF(
      List(),
      List(("id", IntegerType, false), ("name", StringType, true), ("age", IntegerType, true))
    )

    writeTable("testdb.loaddata", df)
  }

  it("appends row without `age` field") {
    df = spark.createDF(
      List((2, "B")),
      List(("id", IntegerType, true), ("name", StringType, true))
    )
    writeTable("testdb.loaddata", df, SaveMode.Append)

    val actualDF = spark.read.format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT).load("testdb.loaddata")
    assertSmallDataFrameEquality(
      actualDF,
      spark.createDF(
        List((2, "B", null)),
        List(("id", IntegerType, true), ("name", StringType, true), ("age", IntegerType, true))
      ))
  }

  it("appends row without `name` field") {
    df = spark.createDF(
      List((3, 30)),
      List(("id", IntegerType, true), ("age", IntegerType, true))
    )
    writeTable("testdb.loaddata", df, SaveMode.Append)

    val actualDF = spark.read.format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT).load("testdb.loaddata")
    assertSmallDataFrameEquality(
      actualDF,
      spark.createDF(
        List((3, null, 30)),
        List(("id", IntegerType, true), ("name", StringType, true), ("age", IntegerType, true))
      ))
  }

  it("should not append row without not nullable `id` field") {
    df = spark.createDF(
      List(("D", 40)),
      List(("name", StringType, true), ("age", IntegerType, true))
    )

    try {
      writeTable("testdb.loaddata", df, SaveMode.Append)
      fail()
    } catch {
      // error code 1364 is `Field 'id' doesn't have a default value`
      case e: Throwable if SQLHelper.isSQLExceptionWithCode(e, List(1364)) =>
    }
  }

  it("appends row with all fields") {
    df = spark.createDF(
      List((5, "E", 50)),
      List(("id", IntegerType, true), ("name", StringType, true), ("age", IntegerType, true))
    )
    writeTable("testdb.loaddata", df, SaveMode.Append)

    val actualDF = spark.read.format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT).load("testdb.loaddata")
    assertSmallDataFrameEquality(
      actualDF,
      spark.createDF(
        List((5, "E", 50)),
        List(("id", IntegerType, true), ("name", StringType, true), ("age", IntegerType, true))
      ))
  }

  it("appends row only with `id` field") {
    df = spark.createDF(List(6), List(("id", IntegerType, true)))
    writeTable("testdb.loaddata", df, SaveMode.Append)

    val actualDF = spark.read.format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT).load("testdb.loaddata")
    assertSmallDataFrameEquality(
      actualDF,
      spark.createDF(
        List((6, null, null)),
        List(("id", IntegerType, true), ("name", StringType, true), ("age", IntegerType, true))
      )
    )
  }

  it("appends row with all fields in wrong order") {
    df = spark.createDF(
      List(("WO", 101, 101)),
      List(("name", StringType, true), ("age", IntegerType, true), ("id", IntegerType, true))
    )
    writeTable("testdb.loaddata", df, SaveMode.Append)

    val actualDF = spark.read.format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT).load("testdb.loaddata")
    assertSmallDataFrameEquality(
      actualDF,
      spark.createDF(
        List((101, "WO", 101)),
        List(("id", IntegerType, true), ("name", StringType, true), ("age", IntegerType, true))
      ))
  }

  it("appends row with `id` and `name` fields in wrong order") {
    df = spark.createDF(
      List(("WO2", 102)),
      List(("name", StringType, true), ("id", IntegerType, true))
    )
    writeTable("testdb.loaddata", df, SaveMode.Append)

    val actualDF = spark.read.format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT).load("testdb.loaddata")
    assertSmallDataFrameEquality(
      actualDF,
      spark.createDF(
        List((102, "WO2", null)),
        List(("id", IntegerType, true), ("name", StringType, true), ("age", IntegerType, true))
      )
    )
  }

  it("should not append row with more fields than expected") {
    df = spark.createDF(
      List((1, "1", 1, 1)),
      List(("id", IntegerType, true),
           ("name", StringType, true),
           ("age", IntegerType, true),
           ("extra", IntegerType, false))
    )

    try {
      writeTable("testdb.loaddata", df, SaveMode.Append)
      fail()
    } catch {
      // error code 1054 is `Unknown column 'extra' in 'field list'`
      case e: Throwable if SQLHelper.isSQLExceptionWithCode(e, List(1054)) =>
    }
  }

  it("should not append row with wrong field name") {
    df = spark.createDF(
      List((1, "1", 1)),
      List(("id", IntegerType, true), ("wrongname", StringType, true), ("age", IntegerType, true)))

    try {
      writeTable("testdb.loaddata", df, SaveMode.Append)
      fail()
    } catch {
      // error code 1054 is `Unknown column 'wrongname' in 'field list'`
      case e: Throwable if SQLHelper.isSQLExceptionWithCode(e, List(1054)) =>
    }
  }

  val timestampValues = List(
    //new Timestamp(999), // Doesn't support [1970-01-01 00:00:00.999]
    //new Timestamp(-10000), // Doesn't support [1969-12-31 23:59:50.0]
    (1, new Timestamp(1000)), // Min supported timestamp [1970-01-01 00:00:01.000]
    (2, new Timestamp(12345)),
    (3, new Timestamp(21474836L)),
    (4, new Timestamp(2147483649L)),
    (5, new Timestamp(214748364900L)),
    (6, new Timestamp(2147483647999L)) // Max supported timestamp [2038-01-19 03:14:07.999]
    //new Timestamp(2147483648000L) // Doesn't support [2038-01-19 03:14:08.000]
  )

  it("[LoadDataWriter] save TimestampType with microseconds") {
    val df = spark.createDF(
      timestampValues,
      List(("id", IntegerType, true), ("time", TimestampType, true))
    )

    df.write
      .format("memsql")
      .mode(SaveMode.Overwrite)
      .option("tableKey.primary", "id")
      .save("testdb.types")

    val actualDF =
      spark.read.format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT).load("testdb.types")
    assertLargeDataFrameEquality(actualDF, df, orderedComparison = false)
  }

  it("[BatchInsertWriter] save TimestampType with microseconds") {
    val df = spark.createDF(
      timestampValues,
      List(("id", IntegerType, true), ("time", TimestampType, true))
    )

    df.write
      .format("memsql")
      .mode(SaveMode.Append)
      .option("tableKey.primary", "id")
      .option("onDuplicateKeySQL", "id = id")
      .save("testdb.types")

    val actualDF =
      spark.read.format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT).load("testdb.types")
    assertLargeDataFrameEquality(actualDF, df, orderedComparison = false)
  }

  it("should fail creating table with NullType") {
    val tableName = "null_type"

    val dfNull = spark.createDF(List(null), List(("id", NullType, true)))
    val writeResult = Try {
      writeTable(s"testdb.$tableName", dfNull, SaveMode.Append)
    }
    assert(writeResult.isFailure)
    assert(
      writeResult.failed.get.getMessage
        .equals(
          "No corresponding MemSQL type found for NullType. If you want to use NullType, please write to an already existing MemSQL table."))
  }

  it("should succeed inserting NullType in existed table") {
    val tableName = "null_type"

    df = spark.createDF(List(1, 2, 3, 4, 5), List(("id", IntegerType, true)))
    writeTable(s"testdb.$tableName", df, SaveMode.Append)

    val dfNull = spark.createDF(List(null), List(("id", NullType, true)))
    writeTable(s"testdb.$tableName", dfNull, SaveMode.Append)
  }
}
