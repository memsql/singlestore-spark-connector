package com.singlestore.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class BatchInsertTest extends IntegrationSuiteBase with BeforeAndAfterEach with BeforeAndAfterAll {
  var df: DataFrame = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    df = spark.createDF(
      List(
        (1, "Jack", 20),
        (2, "Dan", 30),
        (3, "Bob", 15),
        (4, "Alice", 40)
      ),
      List(("id", IntegerType, false), ("name", StringType, true), ("age", IntegerType, true))
    )

    df.write
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .mode(SaveMode.Overwrite)
      .option("tableKey.primary", "id")
      .save("testdb.batchinsert")
  }

  it("insert into a new table") {
    df = spark.createDF(
      List((5, "Eric", 5)),
      List(("id", IntegerType, true), ("name", StringType, true), ("age", IntegerType, true))
    )
    df.write
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .option("tableKey.primary", "id")
      .option("onDuplicateKeySQL", "age = age + 1")
      .option("insertBatchSize", 10)
      .mode(SaveMode.Append)
      .save("testdb.batchinsertnew")

    val actualDF =
      spark.read.format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT).load("testdb.batchinsertnew")
    assertSmallDataFrameEquality(actualDF, df)
  }

  def insertAndCheckContent(batchSize: Int,
                            dfToInsert: List[Any],
                            expectedContent: List[Any]): Unit = {
    df = spark.createDF(
      dfToInsert,
      List(("id", IntegerType, true), ("name", StringType, true), ("age", IntegerType, true))
    )
    insertValues("testdb.batchinsert", df, "age = age + 1", batchSize)

    val actualDF =
      spark.read.format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT).load("testdb.batchinsert")
    assertSmallDataFrameEquality(
      actualDF,
      spark
        .createDF(
          expectedContent,
          List(("id", IntegerType, true), ("name", StringType, true), ("age", IntegerType, true))
        ),
      orderedComparison = false
    )
  }

  it("insert a new row") {
    insertAndCheckContent(
      10,
      List((5, "Eric", 5)),
      List(
        (1, "Jack", 20),
        (2, "Dan", 30),
        (3, "Bob", 15),
        (4, "Alice", 40),
        (5, "Eric", 5)
      )
    )
  }

  it("insert several new rows with small batchSize") {
    insertAndCheckContent(
      2,
      List(
        (5, "Jack", 20),
        (6, "Mark", 30),
        (7, "Fred", 15),
        (8, "Jany", 40),
        (9, "Monica", 5)
      ),
      List(
        (1, "Jack", 20),
        (2, "Dan", 30),
        (3, "Bob", 15),
        (4, "Alice", 40),
        (5, "Jack", 20),
        (6, "Mark", 30),
        (7, "Fred", 15),
        (8, "Jany", 40),
        (9, "Monica", 5)
      )
    )
  }

  it("insert exactly batchSize rows") {
    insertAndCheckContent(
      2,
      List(
        (5, "Jack", 20),
        (6, "Mark", 30)
      ),
      List(
        (1, "Jack", 20),
        (2, "Dan", 30),
        (3, "Bob", 15),
        (4, "Alice", 40),
        (5, "Jack", 20),
        (6, "Mark", 30)
      )
    )
  }

  it("negative batchsize") {
    insertAndCheckContent(
      -2,
      List(
        (5, "Jack", 20),
        (6, "Mark", 30)
      ),
      List(
        (1, "Jack", 20),
        (2, "Dan", 30),
        (3, "Bob", 15),
        (4, "Alice", 40),
        (5, "Jack", 20),
        (6, "Mark", 30)
      )
    )
  }

  it("empty insert") {
    insertAndCheckContent(
      2,
      List(),
      List(
        (1, "Jack", 20),
        (2, "Dan", 30),
        (3, "Bob", 15),
        (4, "Alice", 40)
      )
    )
  }

  it("insert one existing row") {
    insertAndCheckContent(
      2,
      List((1, "Jack", 20)),
      List(
        (1, "Jack", 21),
        (2, "Dan", 30),
        (3, "Bob", 15),
        (4, "Alice", 40)
      )
    )
  }

  it("insert several existing rows") {
    insertAndCheckContent(
      2,
      List(
        (1, "Jack", 20),
        (2, "Dan", 30),
        (3, "Bob", 15),
        (4, "Alice", 40)
      ),
      List(
        (1, "Jack", 21),
        (2, "Dan", 31),
        (3, "Bob", 16),
        (4, "Alice", 41)
      )
    )
  }

  it("insert existing and non existing row") {
    insertAndCheckContent(
      2,
      List(
        (1, "Jack", 20),
        (5, "Mark", 30)
      ),
      List(
        (1, "Jack", 21),
        (2, "Dan", 30),
        (3, "Bob", 15),
        (4, "Alice", 40),
        (5, "Mark", 30)
      )
    )
  }

  it("insert NULL") {
    insertAndCheckContent(
      2,
      List(
        (5, null, null)
      ),
      List(
        (1, "Jack", 20),
        (2, "Dan", 30),
        (3, "Bob", 15),
        (4, "Alice", 40),
        (5, null, null)
      )
    )
  }
}
