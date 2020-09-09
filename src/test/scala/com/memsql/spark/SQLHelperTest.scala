package com.memsql.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterEach
import com.memsql.spark.SQLHelper._
import java.sql.{Date, Timestamp}

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

    spark.executeMemsqlQueryDB("testdb", "DROP TABLE IF EXISTS animal")
    spark.executeMemsqlQueryDB("testdb", "DROP TABLE IF EXISTS numbers")
    spark.executeMemsqlQueryDB("testdb", "DROP TABLE IF EXISTS byte_dates")
    spark.executeMemsqlQuery("DROP DATABASE IF EXISTS test_db_1")
  }

  describe("implicit version") {
    it("global query test") {
      val s = spark.executeMemsqlQuery("SHOW DATABASES")
      val result =
        (for (row <- s)
          yield row.getString(0)).toList
      for (db <- List("memsql", "cluster", "testdb", "information_schema")) {
        assert(result.contains(db))
      }
    }

    it("executeMemsqlQuery with 2 parameters") {
      spark.executeMemsqlQuery("DROP DATABASE IF EXISTS test_db_1")
      spark.executeMemsqlQuery("CREATE DATABASE test_db_1")
    }

    it("executeMemsqlQueryDB with 3 parameters") {
      val res = spark.executeMemsqlQueryDB("testdb", "SELECT * FROM animal")
      val out =
        for (row <- res)
          yield row.getString(1)

      assert(out.toList == List("Cat", "Dog", "CatDog"))
    }

    it("executeMemsqlQuery without explicitly specified db") {
      val res = spark.executeMemsqlQuery("SELECT * FROM animal")
      val out =
        for (row <- res)
          yield row.getString(1)

      assert(out.toList == List("Cat", "Dog", "CatDog"))
    }

    it("executeMemsqlQuery with query params") {
      val params = List("%Cat%", 1, false)
      val res = spark.executeMemsqlQuery(
        "SELECT * FROM animal WHERE name LIKE ? AND id > ? AND domestic = ?",
        "%Cat%",
        1,
        false)
      val out =
        for (row <- res)
          yield row.getString(1)

      assert(out.toList == List("CatDog"))
    }

    it("executeMemsqlQuery with db different from the one found in SparkContext") {
      spark.executeMemsqlQuery(query = "CREATE DATABASE test_db_1")
      spark.executeMemsqlQuery(query = "CREATE TABLE test_db_1.animal (id INT, name TEXT)")

      val res = spark.executeMemsqlQueryDB("test_db_1", "SELECT * FROM animal")
      val out =
        for (row <- res)
          yield row.getString(1)

      assert(out.toList == List())
    }
  }

  it("executeMemsqlQuery with numeric columns") {
    val params = List(1, 22, 2, null)
    val res = spark.executeMemsqlQuery(
      "SELECT * FROM numbers WHERE nLong = ? OR nShort = ? OR nFloat = ? OR nDouble = ?",
      1,
      22,
      2,
      null)
    assert(res.length == 2)
  }

  it("executeMemsqlQuery with byte and date columns") {
    val res = spark.executeMemsqlQuery(
      "SELECT * FROM byte_dates WHERE nByte = ? OR nDate = ? OR nTimestamp = ?",
      2,
      "2015-03-30",
      2000)
    assert(res.length == 2)
  }

  describe("explicit version") {
    it("global query test") {
      val s = executeMemsqlQuery(spark, "SHOW DATABASES")
      val result =
        (for (row <- s)
          yield row.getString(0)).toList
      for (db <- List("memsql", "cluster", "testdb", "information_schema")) {
        assert(result.contains(db))
      }
    }

    it("executeMemsqlQuery with 2 parameters") {
      executeMemsqlQuery(spark, "DROP DATABASE IF EXISTS test_db_1")
      executeMemsqlQuery(spark, "CREATE DATABASE test_db_1")
    }

    it("executeMemsqlQueryDB with 3 parameters") {
      val res = executeMemsqlQueryDB(spark, "testdb", "SELECT * FROM animal")
      val out =
        for (row <- res)
          yield row.getString(1)

      assert(out.toList == List("Cat", "Dog", "CatDog"))
    }

    it("executeMemsqlQuery without explicitly specified db") {
      val res = executeMemsqlQuery(spark, "SELECT * FROM animal")
      val out =
        for (row <- res)
          yield row.getString(1)

      assert(out.toList == List("Cat", "Dog", "CatDog"))
    }

    it("executeMemsqlQuery with query params") {
      val params = List("%Cat%", 1, false)
      val res =
        executeMemsqlQuery(spark,
                           "SELECT * FROM animal WHERE name LIKE ? AND id > ? AND domestic = ?",
                           "%Cat%",
                           1,
                           false)
      val out =
        for (row <- res)
          yield row.getString(1)

      assert(out.toList == List("CatDog"))
    }

    it("executeMemsqlQuery with db different from the one found in SparkContext") {
      executeMemsqlQuery(spark, query = "CREATE DATABASE test_db_1")
      executeMemsqlQuery(spark, query = "CREATE TABLE test_db_1.animal (id INT, name TEXT)")

      val res = executeMemsqlQueryDB(spark, "test_db_1", "SELECT * FROM animal")
      val out =
        for (row <- res)
          yield row.getString(1)

      assert(out.toList == List())
    }

    it("executeMemsqlQuery with numeric columns") {
      val params = List(1, 22, 2, null)
      val res = executeMemsqlQuery(
        spark,
        "SELECT * FROM numbers WHERE nLong = ? OR nShort = ? OR nFloat = ? OR nDouble = ?",
        1,
        22,
        2,
        null)
      assert(res.length == 2)
    }

    it("executeMemsqlQuery with byte and date columns") {
      val res =
        executeMemsqlQuery(
          spark,
          "SELECT * FROM byte_dates WHERE nByte = ? OR nDate = ? OR nTimestamp = ?",
          2,
          "2015-03-30",
          2000)
      assert(res.length == 2)
    }
  }
}
