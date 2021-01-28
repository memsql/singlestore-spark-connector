package com.singlestore.spark

import java.sql.SQLTransientConnectionException

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.util.Try

class MaxErrorsTest extends IntegrationSuiteBase with BeforeAndAfterEach with BeforeAndAfterAll {

  def testMaxErrors(tableName: String, maxErrors: Int, duplicateItems: Int): Unit = {
    val df = spark.createDF(
      List.fill(duplicateItems + 1)((1, "Alice", 213: BigDecimal)),
      List(("id", IntegerType, true),
           ("name", StringType, false),
           ("age", DecimalType(10, 0), true))
    )
    val result = Try {
      df.write
        .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
        .option("tableKey.primary", "name")
        .option("maxErrors", maxErrors)
        .mode(SaveMode.Ignore)
        .save(s"testdb.$tableName")
    }
    if (duplicateItems > maxErrors) {
      assert(result.isFailure)
      result.failed.get.getCause match {
        case _: SQLTransientConnectionException =>
        case _                                  => fail("SQLTransientConnectionException should be thrown")
      }
    } else {
      assert(result.isSuccess)
    }
  }

  describe("small dataset") {
    it("hit maxErrors") {
      testMaxErrors("hitMaxErrorsSmall", 1, 2)
    }

    it("not hit maxErrors") {
      testMaxErrors("notHitMaxErrorsSmall", 1, 1)
    }
  }

  describe("big dataset") {
    it("hit maxErrors") {
      testMaxErrors("hitMaxErrorsBig", 10000, 10001)
    }

    it("not hit maxErrors") {
      testMaxErrors("notHitMaxErrorsBig", 10000, 10000)
    }
  }

  it("wrong configuration") {
    val df = spark.createDF(
      List((1, "Alice", 213: BigDecimal)),
      List(("id", IntegerType, true),
           ("name", StringType, false),
           ("age", DecimalType(10, 0), true))
    )
    val result = Try {
      df.write
        .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
        .option("onDuplicateKeySQL", "id=id")
        .option("maxErrors", 1)
        .mode(SaveMode.Ignore)
        .save(s"testdb.someTable")
    }
    assert(result.isFailure)
    result.failed.get match {
      case ex: IllegalArgumentException
          if ex.getMessage.equals("can't use both `onDuplicateKeySQL` and `maxErrors` options") =>
        succeed
      case _ => fail()
    }
  }
}
