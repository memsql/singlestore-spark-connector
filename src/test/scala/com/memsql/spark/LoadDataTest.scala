package com.memsql.spark

import java.sql.{SQLException}

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

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

  def isSQLExceptionWithCode(e: Throwable, code: Integer): Boolean = e match {
    case e: SQLException if e.getErrorCode == code => true
    case e if e.getCause != null                   => isSQLExceptionWithCode(e.getCause, code)
    case _                                         => false
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
      case e: Throwable if isSQLExceptionWithCode(e, 1364) =>
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
      case e: Throwable if isSQLExceptionWithCode(e, 1054) =>
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
      case e: Throwable if isSQLExceptionWithCode(e, 1054) =>
    }
  }
}
