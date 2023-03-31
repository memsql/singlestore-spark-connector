package com.singlestore.spark

import java.sql.{Date, Timestamp}

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.singlestore.spark.SQLHelper._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterEach

class SQLHelperTest extends IntegrationSuiteBase with BeforeAndAfterEach {
  var df: DataFrame = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    df = spark.createDF(
      List((1, "Cat", true), (2, "Dog", true), (3, "CatDog", false)),
      List(("id", IntegerType, true), ("name", StringType, true), ("domestic", BooleanType, true))
    )
    writeTable("testdb.animal", df)

    df = spark.createDF(
      List((1: Long, 2: Short, 3: Float, 4: Double),
           (-2: Long, 22: Short, 2.0.toFloat, 5.1: Double),
           (3: Long, 4: Short, -0.11.toFloat, 66.77: Double)),
      List(("nLong", LongType, true),
           ("nShort", ShortType, true),
           ("nFloat", FloatType, true),
           ("nDouble", DoubleType, true))
    )
    writeTable("testdb.numbers", df)

    df = spark.createDF(
      List((1: Byte, Date.valueOf("2015-03-30"), new Timestamp(2147483649L)),
           (2: Byte, Date.valueOf("2020-09-09"), new Timestamp(1000))),
      List(("nByte", ByteType, true),
           ("nDate", DateType, true),
           ("nTimestamp", TimestampType, true))
    )
    writeTable("testdb.byte_dates", df)
  }

  override def afterEach(): Unit = {
    super.afterEach()

    spark.executeSinglestoreQueryDB("testdb", "DROP TABLE IF EXISTS animal")
    spark.executeSinglestoreQueryDB("testdb", "DROP TABLE IF EXISTS numbers")
    spark.executeSinglestoreQueryDB("testdb", "DROP TABLE IF EXISTS byte_dates")
    spark.executeSinglestoreQuery("DROP DATABASE IF EXISTS test_db_1")
  }

  describe("implicit version") {
    it("global query test") {
      val s = spark.executeSinglestoreQuery("SHOW DATABASES")
      val result =
        (for (row <- s)
          yield row.getString(0)).toList
      for (db <- List("memsql", "cluster", "testdb", "information_schema")) {
        assert(result.contains(db))
      }
    }

    it("executeSinglestoreQuery with 2 parameters") {
      spark.executeSinglestoreQuery("DROP DATABASE IF EXISTS test_db_1")
      spark.executeSinglestoreQuery("CREATE DATABASE test_db_1")
    }

    it("executeSinglestoreQueryDB with 3 parameters") {
      val res = spark.executeSinglestoreQueryDB("testdb", "SELECT * FROM animal")
      val out =
        for (row <- res)
          yield row.getString(1)

      assert(out.toList.sorted == List("Cat", "CatDog", "Dog"))
    }

    it("executeSingleStoreQuery without explicitly specified db") {
      val res = spark.executeSinglestoreQuery("SELECT * FROM animal")
      val out =
        for (row <- res)
          yield row.getString(1)

      assert(out.toList.sorted == List("Cat", "CatDog", "Dog"))
    }

    it("executeSinglestoreQuery with query params") {
      val params = List("%Cat%", 1, false)
      val res = spark.executeSinglestoreQuery(
        "SELECT * FROM animal WHERE name LIKE ? AND id > ? AND domestic = ?",
        "%Cat%",
        1,
        false)
      val out =
        for (row <- res)
          yield row.getString(1)

      assert(out.toList == List("CatDog"))
    }

    it("executeSinglestoreQuery with db different from the one found in SparkContext") {
      spark.executeSinglestoreQuery(query = "CREATE DATABASE test_db_1")
      spark.executeSinglestoreQuery(query = "CREATE TABLE test_db_1.animal (id INT, name TEXT)")

      val res = spark.executeSinglestoreQueryDB("test_db_1", "SELECT * FROM animal")
      val out =
        for (row <- res)
          yield row.getString(1)

      assert(out.toList == List())
    }
  }

  it("executeSinglestoreQuery with numeric columns") {
    val params = List(1, 22, 2, null)
    val res = spark.executeSinglestoreQuery(
      "SELECT * FROM numbers WHERE nLong = ? OR nShort = ? OR nFloat = ? OR nDouble = ?",
      1,
      22,
      2,
      null)
    assert(res.length == 2)
  }

  it("executeSinglestoreQuery with byte and date columns") {
    val res = spark.executeSinglestoreQuery(
      "SELECT * FROM byte_dates WHERE nByte = ? OR nDate = ? OR nTimestamp = ?",
      2,
      "2015-03-30",
      2000)
    assert(res.length == 2)
  }

  describe("explicit version") {
    it("global query test") {
      val s = executeSinglestoreQuery(spark, "SHOW DATABASES")
      val result =
        (for (row <- s)
          yield row.getString(0)).toList
      for (db <- List("memsql", "cluster", "testdb", "information_schema")) {
        assert(result.contains(db))
      }
    }

    it("executeSinglestoreQuery with 2 parameters") {
      executeSinglestoreQuery(spark, "DROP DATABASE IF EXISTS test_db_1")
      executeSinglestoreQuery(spark, "CREATE DATABASE test_db_1")
    }

    it("executeSinglestoreQueryDB with 3 parameters") {
      val res = executeSinglestoreQueryDB(spark, "testdb", "SELECT * FROM animal")
      val out =
        for (row <- res)
          yield row.getString(1)

      assert(out.toList.sorted == List("Cat", "CatDog", "Dog"))
    }

    it("executeSinglestoreQuery without explicitly specified db") {
      val res = executeSinglestoreQuery(spark, "SELECT * FROM animal")
      val out =
        for (row <- res)
          yield row.getString(1)

      assert(out.toList.sorted == List("Cat", "CatDog", "Dog"))
    }

    it("executeSinglestoreQuery with query params") {
      val params = List("%Cat%", 1, false)
      val res =
        executeSinglestoreQuery(
          spark,
          "SELECT * FROM animal WHERE name LIKE ? AND id > ? AND domestic = ?",
          "%Cat%",
          1,
          false)
      val out =
        for (row <- res)
          yield row.getString(1)

      assert(out.toList == List("CatDog"))
    }

    it("executeSinglestoreQuery with db different from the one found in SparkContext") {
      executeSinglestoreQuery(spark, query = "CREATE DATABASE test_db_1")
      executeSinglestoreQuery(spark, query = "CREATE TABLE test_db_1.animal (id INT, name TEXT)")

      val res = executeSinglestoreQueryDB(spark, "test_db_1", "SELECT * FROM animal")
      val out =
        for (row <- res)
          yield row.getString(1)

      assert(out.toList == List())
    }

    it("executeSinglestoreQuery with numeric columns") {
      val params = List(1, 22, 2, null)
      val res = executeSinglestoreQuery(
        spark,
        "SELECT * FROM numbers WHERE nLong = ? OR nShort = ? OR nFloat = ? OR nDouble = ?",
        1,
        22,
        2,
        null)
      assert(res.length == 2)
    }

    it("executeSinglestoreQuery with byte and date columns") {
      val res =
        executeSinglestoreQuery(
          spark,
          "SELECT * FROM byte_dates WHERE nByte = ? OR nDate = ? OR nTimestamp = ?",
          2,
          "2015-03-30",
          2000)
      assert(res.length == 2)
    }
  }
}
