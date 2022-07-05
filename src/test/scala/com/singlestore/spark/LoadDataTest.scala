package com.singlestore.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types.{DecimalType, IntegerType, NullType, StringType}
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

    val actualDF =
      spark.read.format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT).load("testdb.loaddata")
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

    val actualDF =
      spark.read.format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT).load("testdb.loaddata")
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
      case e: Throwable if TestHelper.isSQLExceptionWithCode(e, List(1364)) =>
    }
  }

  it("appends row with all fields") {
    df = spark.createDF(
      List((5, "E", 50)),
      List(("id", IntegerType, true), ("name", StringType, true), ("age", IntegerType, true))
    )
    writeTable("testdb.loaddata", df, SaveMode.Append)

    val actualDF =
      spark.read.format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT).load("testdb.loaddata")
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

    val actualDF =
      spark.read.format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT).load("testdb.loaddata")
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

    val actualDF =
      spark.read.format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT).load("testdb.loaddata")
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

    val actualDF =
      spark.read.format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT).load("testdb.loaddata")
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
      case e: Throwable if TestHelper.isSQLExceptionWithCode(e, List(1054)) =>
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
      case e: Throwable if TestHelper.isSQLExceptionWithCode(e, List(1054)) =>
    }
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
          "No corresponding SingleStore type found for NullType. If you want to use NullType, please write to an already existing SingleStore table."))
  }

  it("should succeed inserting NullType in existed table") {
    val tableName = "null_type"

    df = spark.createDF(List(1, 2, 3, 4, 5), List(("id", IntegerType, true)))
    writeTable(s"testdb.$tableName", df, SaveMode.Append)

    val dfNull = spark.createDF(List(null), List(("id", NullType, true)))
    writeTable(s"testdb.$tableName", dfNull, SaveMode.Append)
  }

  it("should write BigDecimal with Avro serializing") {
    val tableName = "bigDecimalAvro"
    val df = spark.createDF(
      List((1, "Alice", 213: BigDecimal)),
      List(("id", IntegerType, true), ("name", StringType, true), ("age", DecimalType(10, 0), true))
    )
    df.write
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .mode(SaveMode.Overwrite)
      .option(SinglestoreOptions.LOAD_DATA_FORMAT, "avro")
      .save(s"testdb.$tableName")

    val actualDF =
      spark.read.format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT).load(s"testdb.$tableName")
    assertLargeDataFrameEquality(actualDF, df)
  }

  it("should work with `memsql` and `com.memsql.spark` source") {
    df = spark.createDF(
      List((5, "E", 50)),
      List(("id", IntegerType, true), ("name", StringType, true), ("age", IntegerType, true))
    )
    writeTable("testdb.loaddata", df, SaveMode.Append)

    val actualDFShort =
      spark.read.format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT).load("testdb.loaddata")
    assertSmallDataFrameEquality(
      actualDFShort,
      spark.createDF(
        List((5, "E", 50)),
        List(("id", IntegerType, true), ("name", StringType, true), ("age", IntegerType, true))
      ))
    val actualDF =
      spark.read.format(DefaultSource.MEMSQL_SOURCE_NAME).load("testdb.loaddata")
    assertSmallDataFrameEquality(
      actualDF,
      spark.createDF(
        List((5, "E", 50)),
        List(("id", IntegerType, true), ("name", StringType, true), ("age", IntegerType, true))
      ))
  }

  it("non-existing column") {
    executeQueryWithLog("DROP TABLE IF EXISTS loaddata")
    executeQueryWithLog("CREATE TABLE loaddata(id INT, name TEXT)")

    df = spark.createDF(
      List((5, "EBCEFGRHFED" * 10000000, 50)),
      List(("id", IntegerType, true), ("name", StringType, true), ("age", IntegerType, true))
    )

    try {
      writeTable("testdb.loaddata", df, SaveMode.Append)
      fail()
    } catch {
      case e: Exception if e.getMessage.contains("Unknown column 'age' in 'field list'") =>
    }
  }
}
