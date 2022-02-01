package com.singlestore.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.singlestore.spark.SQLHelper.QueryMethods
import com.singlestore.spark.SinglestoreOptions.{OVERWRITE_BEHAVIOR, TRUNCATE}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StringType}

import scala.List
import scala.util.Try

class SQLOverwriteTest extends IntegrationSuiteBase {

  val dbName    = "testdb"
  val tableName = "overwrite_table"

  override def beforeEach(): Unit = {
    super.beforeEach()
    dropTable(tableName)
  }

  def dropTable(tableName: String): Unit =
    executeQueryWithLog(s"drop table if exists $dbName.$tableName")

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
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .mode(SaveMode.Overwrite)
      .option("tableKey.primary", "id")
      .save(s"${dbName}.${tableName}")

    val dfAfterOperation = spark.createDF(
      dfAfter,
      schema
    )
    dfAfterOperation.write
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .options(options)
      .mode(mode)
      .save(s"${dbName}.${tableName}")

    val dfExpected = spark.createDF(
      expected,
      schema
    )
    val actualDF =
      spark.read.format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT).load(s"$dbName.$tableName")
    assertLargeDataFrameEquality(actualDF, dfExpected, orderedComparison = false)
  }

  describe("dropAndCreate option") {

    it("success drop and create table with overwriteBehavior option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob"), (3, "Eve")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        Map(SinglestoreOptions.OVERWRITE_BEHAVIOR -> "dropAndCreate")
      )
    }
  }

  describe("truncate option") {

    it("success truncate with overwriteBehavior option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob"), (3, "Eve")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        Map(SinglestoreOptions.OVERWRITE_BEHAVIOR -> "truncate")
      )
    }

    it("success truncate with truncate option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob"), (3, "Eve")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        Map(SinglestoreOptions.TRUNCATE -> "true")
      )
    }
  }

  describe("merge option") {

    it("success merge without onDuplicateKeySQL option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob")),
        List((2, "Charlie"), (3, "John")),
        List((1, "Alice"), (2, "Charlie"), (3, "John")),
        Map(SinglestoreOptions.OVERWRITE_BEHAVIOR -> "merge")
      )
    }

    it("success merge with onDuplicateKeySQL option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob")),
        List((2, "Charlie"), (3, "John")),
        List((1, "Alice"), (2, "Duplicate"), (3, "John")),
        Map(SinglestoreOptions.ON_DUPLICATE_KEY_SQL -> "name = 'Duplicate'"),
        SaveMode.Append
      )
    }

    it("skip merge due to ignore save mode") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob")),
        List((2, "Charlie"), (3, "John")),
        List((1, "Alice"), (2, "Bob"), (3, "John")),
        Map(SinglestoreOptions.OVERWRITE_BEHAVIOR -> "merge"),
        SaveMode.Ignore
      )
    }

    it("still do merge if onDuplicateKeySQL option is defined") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob")),
        List((2, "Charlie"), (3, "John")),
        List((1, "Alice"), (2, "Duplicate"), (3, "John")),
        Map(SinglestoreOptions.ON_DUPLICATE_KEY_SQL -> "name = 'Duplicate'"),
        SaveMode.Ignore
      )
    }

    it("merge on overwrite with columnstore unique constraints") {
      if (version.atLeast("7.3.0")) {
        spark.executeSinglestoreQuery(query =
          s"CREATE TABLE $dbName.$tableName(a INT, b INT, SHARD KEY (a), UNIQUE KEY(a) USING HASH, SORT KEY(a))")
        spark.executeSinglestoreQuery(
          query = s"INSERT INTO $dbName.$tableName VALUES (1, 1), (2, 2)")

        val df = spark.createDF(
          List(
            (2, 4),
            (3, 3),
          ),
          List(("a", IntegerType, false), ("b", IntegerType, true))
        )

        df.write
          .format("singlestore")
          .mode(SaveMode.Overwrite)
          .option("overwriteBehavior", "merge")
          .save(s"$dbName.$tableName")

        val resultDf = spark.read.format("singlestore").load(s"$dbName.$tableName")

        assertSmallDataFrameEquality(resultDf,
                                     spark.createDF(
                                       List(
                                         (1, 1),
                                         (2, 4),
                                         (3, 3),
                                       ),
                                       List(("a", IntegerType, true), ("b", IntegerType, true))
                                     ),
                                     orderedComparison = false)
      }
    }
  }

  describe("case sensitive success") {

    it("dropandcreate option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob"), (3, "Eve")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        Map(SinglestoreOptions.OVERWRITE_BEHAVIOR -> "dropandcreate")
      )
    }

    it("DROPaNDcREATE option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob"), (3, "Eve")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        Map(SinglestoreOptions.OVERWRITE_BEHAVIOR -> "DROPaNDcREATE")
      )
    }

    it("Truncate option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob"), (3, "Eve")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        Map(SinglestoreOptions.OVERWRITE_BEHAVIOR -> "Truncate")
      )
    }

    it("tRUNCATE option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob"), (3, "Eve")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        List((4, "Charlie"), (5, "John"), (6, "Mary")),
        Map(SinglestoreOptions.OVERWRITE_BEHAVIOR -> "tRUNCATE")
      )
    }

    it("Merge option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob")),
        List((2, "Charlie"), (3, "John")),
        List((1, "Alice"), (2, "Charlie"), (3, "John")),
        Map(SinglestoreOptions.OVERWRITE_BEHAVIOR -> "Merge")
      )
    }

    it("meRgE option") {
      insertAndAssertEquality(
        List((1, "Alice"), (2, "Bob")),
        List((2, "Charlie"), (3, "John")),
        List((1, "Alice"), (2, "Charlie"), (3, "John")),
        Map(SinglestoreOptions.OVERWRITE_BEHAVIOR -> "meRgE")
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
          Map(SinglestoreOptions.OVERWRITE_BEHAVIOR -> "dropAndCreatea")
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
          Map(SinglestoreOptions.OVERWRITE_BEHAVIOR -> "dropAndCreate",
              SinglestoreOptions.TRUNCATE           -> "true")
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

    it("duplicate key error if save mode append") {
      val result = Try {
        insertAndAssertEquality(
          List((1, "Alice"), (2, "Bob"), (3, "Eve")),
          List((2, "Charlie"), (3, "John")),
          List(),
          Map(),
          SaveMode.Append
        )
      }
      assert(result.isFailure)
      /* Error code description:
        1062 = duplicate key error
       * */
      TestHelper.isSQLExceptionWithCode(result.failed.get, List(1062))
    }
  }

  describe("on duplicate key update") {

    def createTable(isColumnstore: Boolean): Unit = {
      val createTableQuery = if (isColumnstore) {
        s"CREATE TABLE $dbName.$tableName( srt int, suk int, nuk int, sort key(srt), shard key(suk), key(nuk) using hash, unique key(suk) using hash);"
      } else {
        val rowstore = if (version.atLeast("7.5.0") && isColumnstore) {
          "ROWSTORE"
        } else {
          ""
        }
        s"CREATE $rowstore TABLE $dbName.$tableName( srt int, suk int, nuk int, sort key(srt), shard key(suk), key(nuk) using hash, key(suk) using hash);"

      }
      spark.executeSinglestoreQuery(query = createTableQuery)
      spark.executeSinglestoreQuery(query = s"INSERT INTO $dbName.$tableName VALUES (1, 1, 1)")
    }

    def assertOnDuplicateKeyUpdateResult[U, T <: Product](rowData: List[U],
                                                          fields: List[T]): Unit = {
      assertSmallDataFrameEquality(spark.read.format("singlestore").load(s"$dbName.$tableName"),
                                   spark.createDF(rowData, fields))
    }

    def onDuplicateKeyUpdate(isColumnstore: Boolean): Unit = {
      val fields =
        List(("srt", IntegerType, true), ("suk", IntegerType, true), ("nuk", IntegerType, true))
      val df = spark.read.format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT).load(s"$tableName")
      df.write
        .format("singlestore")
        .option("onDuplicateKeySQL", "srt = 2")
        .option("insertBatchSize", 300)
        .mode(SaveMode.Append)
        .save(s"$dbName.$tableName")

      if (isColumnstore) {
        assertOnDuplicateKeyUpdateResult(
          List(
            (2, 1, 1),
          ),
          fields
        )
      } else {
        assertOnDuplicateKeyUpdateResult(
          List(
            (1, 1, 1),
            (1, 1, 1),
          ),
          fields
        )
      }

      df.write
        .format("singlestore")
        .option("onDuplicateKeySQL", "nuk = 2")
        .option("insertBatchSize", 300)
        .mode(SaveMode.Append)
        .save(s"$dbName.$tableName")

      if (isColumnstore) {
        assertOnDuplicateKeyUpdateResult(
          List(
            (2, 1, 2),
          ),
          fields
        )
      } else {
        assertOnDuplicateKeyUpdateResult(
          List(
            (1, 1, 1),
            (1, 1, 1),
            (1, 1, 1),
            (1, 1, 1),
          ),
          fields
        )
      }

      if (isColumnstore) {
        val result = Try {
          df.write
            .format("singlestore")
            .option("onDuplicateKeySQL", "suk = 2")
            .option("insertBatchSize", 300)
            .mode(SaveMode.Append)
            .save(s"$dbName.$tableName")
        }
        assert(result.isFailure)
      }
    }

    it("success update with unique key") {
      if (version.atLeast("7.3.0")) {
        createTable(true)
        onDuplicateKeyUpdate(true)
      }
    }

    it("success update without unique key") {
      if (version.atLeast("7.3.0")) {
        createTable(false)
        onDuplicateKeyUpdate(false)
      }
    }
  }
}
