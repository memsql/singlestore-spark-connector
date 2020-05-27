package com.memsql.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

class BenchmarkSerializingTest extends IntegrationSuiteBase {

  val dbName    = "testdb"
  val tableName = "avro_table"

  val writeIterations = 10

  val smallDataCount  = 1
  val mediumDataCount = 1000
  val largeDataCount  = 100000

  val smallSchema = List(("id", StringType, false))
  val mediumSchema = List(("id", StringType, false),
                          ("name", StringType, false),
                          ("surname", StringType, false),
                          ("age", IntegerType, false))
  val largeSchema = List(
    ("id", StringType, false),
    ("name", StringType, false),
    ("surname", StringType, false),
    ("someString", StringType, false),
    ("anotherString", StringType, false),
    ("age", IntegerType, false),
    ("secondNumber", IntegerType, false),
    ("thirdNumber", LongType, false)
  )

  def generateSmallData(index: Int)  = s"$index"
  def generateMediumData(index: Int) = (s"$index", s"name$index", s"surname$index", index)
  def generateLargeData(index: Int) =
    (s"$index",
     s"name$index",
     s"surname$index",
     s"someString$index",
     s"anotherString$index",
     index,
     index + 1,
     index * 2L)

  val generateDataMap: Map[List[scala.Product], Int => Any] = Map(
    smallSchema  -> generateSmallData,
    mediumSchema -> generateMediumData,
    largeSchema  -> generateLargeData
  )

  override def beforeEach(): Unit = {
    super.beforeEach()
    executeQuery(s"drop table if exists $dbName.$tableName")
  }

  def doWriteOperation(dataFrame: DataFrame, options: Map[String, String]): Long = {
    val startTime = System.currentTimeMillis()
    for (_ <- 1 to writeIterations) {
      dataFrame.write
        .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
        .mode(SaveMode.Append)
        .options(options)
        .save(s"$dbName.$tableName")
    }
    val endTime = System.currentTimeMillis()
    endTime - startTime
  }

  def doTestOperation(dataCount: Int,
                      schema: List[scala.Product],
                      options: Map[String, String]): Unit = {
    val dataFrameValues = List.tabulate(dataCount)(generateDataMap(schema))
    val dataFrame =
      spark.createDF(dataFrameValues, schema)
    val timeSpend = doWriteOperation(dataFrame, options)
    print(s"Time spend for $writeIterations iterations: $timeSpend")
  }

  describe("Avro testing") {

    it("small data | small schema") {
      doTestOperation(smallDataCount, smallSchema, Map(MemsqlOptions.LOAD_DATA_FORMAT -> "avro"))
    }

    it("medium data | small schema") {
      doTestOperation(mediumDataCount, smallSchema, Map(MemsqlOptions.LOAD_DATA_FORMAT -> "avro"))
    }

    it("large data | small schema") {
      doTestOperation(largeDataCount, smallSchema, Map(MemsqlOptions.LOAD_DATA_FORMAT -> "avro"))
    }

    it("small data | medium schema") {
      doTestOperation(smallDataCount, mediumSchema, Map(MemsqlOptions.LOAD_DATA_FORMAT -> "avro"))
    }

    it("medium data | medium schema") {
      doTestOperation(mediumDataCount, mediumSchema, Map(MemsqlOptions.LOAD_DATA_FORMAT -> "avro"))
    }

    it("large data | medium schema") {
      doTestOperation(largeDataCount, mediumSchema, Map(MemsqlOptions.LOAD_DATA_FORMAT -> "avro"))
    }

    it("small data | large schema") {
      doTestOperation(smallDataCount, largeSchema, Map(MemsqlOptions.LOAD_DATA_FORMAT -> "avro"))
    }

    it("medium data | large schema") {
      doTestOperation(mediumDataCount, largeSchema, Map(MemsqlOptions.LOAD_DATA_FORMAT -> "avro"))
    }

    it("large data | large schema") {
      doTestOperation(largeDataCount, largeSchema, Map(MemsqlOptions.LOAD_DATA_FORMAT -> "avro"))
    }
  }

  describe("CSV testing") {
    it("small data | small schema") {
      doTestOperation(smallDataCount, smallSchema, Map.empty)
    }

    it("medium data | small schema") {
      doTestOperation(mediumDataCount, smallSchema, Map.empty)
    }

    it("large data | small schema") {
      doTestOperation(largeDataCount, smallSchema, Map.empty)
    }

    it("small data | medium schema") {
      doTestOperation(smallDataCount, mediumSchema, Map.empty)
    }

    it("medium data | medium schema") {
      doTestOperation(mediumDataCount, mediumSchema, Map.empty)
    }

    it("large data | medium schema") {
      doTestOperation(largeDataCount, mediumSchema, Map.empty)
    }

    it("small data | large schema") {
      doTestOperation(smallDataCount, largeSchema, Map.empty)
    }

    it("medium data | large schema") {
      doTestOperation(mediumDataCount, largeSchema, Map.empty)
    }

    it("large data | large schema") {
      doTestOperation(largeDataCount, largeSchema, Map.empty)
    }
  }
}
