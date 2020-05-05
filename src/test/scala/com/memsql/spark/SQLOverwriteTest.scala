package com.memsql.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.memsql.spark.MemsqlOptions.{OVERWRITE_BEHAVIOR, TRUNCATE}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StringType}

import scala.util.Try

class SQLOverwriteTest extends IntegrationSuiteBase {

  val dbName    = "testdb"
  val tableName = "overwrite_table"

  override def beforeEach(): Unit = {
    super.beforeEach()
    dropTable(tableName)
  }

  def dropTable(tableName: String): Unit = executeQuery(s"drop table if exists $dbName.$tableName")

  def insertAndAssertEquality(dfBefore: List[(Integer, String)],
                              dfAfter: List[(Integer, String)],
                              expected: List[(Integer, String)],
                              options: Map[String, String],
                              mode: SaveMode = SaveMode.Overwrite): Unit = {
    val schema = List(("id", IntegerType, true), ("name", StringType, true))
    val dfBeforeOperation = spark.createDF(
      dfBefore,
      schema
    )
    dfBeforeOperation.write
      .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
      .mode(SaveMode.Overwrite)
      .option("tableKey.primary", "id")
      .save(s"${dbName}.${tableName}")

    val dfAfterOperation = spark.createDF(
      dfAfter,
      schema
    )
    dfAfterOperation.write
      .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
      .options(options)
      .mode(mode)
      .save(s"${dbName}.${tableName}")

    val dfExpected = spark.createDF(
      expected,
      schema
    )
    val actualDF =
      spark.read.format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT).load(s"$dbName.$tableName")
    assertLargeDataFrameEquality(actualDF, dfExpected, orderedComparison = false)
  }

  describe("dropAndCreate option") {

    it("success drop and create table with overwriteBehavior option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob"), (3, "Eve")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        Map(MemsqlOptions.OVERWRITE_BEHAVIOR -> "dropAndCreate")
      )
    }
  }

  describe("truncate option") {

    it("success truncate with overwriteBehavior option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob"), (3, "Eve")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        Map(MemsqlOptions.OVERWRITE_BEHAVIOR -> "truncate")
      )
    }

    it("success truncate with truncate option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob"), (3, "Eve")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        Map(MemsqlOptions.TRUNCATE -> "true")
      )
    }
  }

  describe("merge option") {

    it("success merge without onDuplicateKeySQL option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob")),
        List((2, "Charlie"), (3, "John")),
        List((1, "Alice"), (2, "Charlie"), (3, "John")),
        Map(MemsqlOptions.OVERWRITE_BEHAVIOR -> "merge")
      )
    }

    it("success merge with onDuplicateKeySQL option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob")),
        List((2, "Charlie"), (3, "John")),
        List((1, "Alice"), (2, "Duplicate"), (3, "John")),
        Map(MemsqlOptions.ON_DUPLICATE_KEY_SQL -> "name = 'Duplicate'"),
        SaveMode.Append
      )
    }
  }

  describe("case sensitive success") {

    it("dropandcreate option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob"), (3, "Eve")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        Map(MemsqlOptions.OVERWRITE_BEHAVIOR -> "dropandcreate")
      )
    }

    it("DROPaNDcREATE option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob"), (3, "Eve")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        Map(MemsqlOptions.OVERWRITE_BEHAVIOR -> "DROPaNDcREATE")
      )
    }

    it("Truncate option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob"), (3, "Eve")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        Map(MemsqlOptions.OVERWRITE_BEHAVIOR -> "Truncate")
      )
    }

    it("tRUNCATE option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob"), (3, "Eve")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        Map(MemsqlOptions.OVERWRITE_BEHAVIOR -> "tRUNCATE")
      )
    }

    it("Merge option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob")),
        List((2, "Charlie"), (3, "John")),
        List((1, "Alice"), (2, "Charlie"), (3, "John")),
        Map(MemsqlOptions.OVERWRITE_BEHAVIOR -> "Merge")
      )
    }

    it("meRgE option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob")),
        List((2, "Charlie"), (3, "John")),
        List((1, "Alice"), (2, "Charlie"), (3, "John")),
        Map(MemsqlOptions.OVERWRITE_BEHAVIOR -> "meRgE")
      )
    }
  }

  describe("failure during") {

    it("setting wrong overwriteBehavior option") {
      val result = Try {
        insertAndAssertEquality(
          List((1, "Alice"), (2, "Bob"), (3, "Eve")),
          List((4, "Charlie"), (5, "John"), (6, "Mary")),
          List((4, "Charlie"), (5, "John"), (6, "Mary")),
          Map(MemsqlOptions.OVERWRITE_BEHAVIOR -> "dropAndCreatea")
        )
      }
      assert(result.isFailure)
      result.failed.get match {
        case ex: IllegalArgumentException
            if ex.getMessage.equals("Illegal argument for `overwriteBehavior` option") =>
          succeed
        case _ => fail()
      }
    }

    it("set up both truncate and overwriteBehavior options") {
      val result = Try {
        insertAndAssertEquality(
          List((1, "Alice"), (2, "Bob"), (3, "Eve")),
          List((4, "Charlie"), (5, "John"), (6, "Mary")),
          List((4, "Charlie"), (5, "John"), (6, "Mary")),
          Map(MemsqlOptions.OVERWRITE_BEHAVIOR -> "dropAndCreate", MemsqlOptions.TRUNCATE -> "true")
        )
      }
      assert(result.isFailure)
      result.failed.get match {
        case ex: IllegalArgumentException
            if ex.getMessage.equals(
              s"can't use both `$TRUNCATE` and `$OVERWRITE_BEHAVIOR` options, please use just `$OVERWRITE_BEHAVIOR` option instead.") =>
          succeed
        case _ => fail()
      }
    }
  }

}
