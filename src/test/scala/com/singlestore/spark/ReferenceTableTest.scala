package com.singlestore.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.singlestore.spark.SQLHelper._

import scala.util.Try

class ReferenceTableTest extends IntegrationSuiteBase {

  val childAggregatorHost = "localhost"
  val childAggregatorPort = "5508"

  val dbName                  = "testdb"
  val commonCollectionName    = "test_table"
  val referenceCollectionName = "reference_table"

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Set child aggregator as a dmlEndpoint
    spark.conf
      .set("spark.datasource.singlestore.dmlEndpoints",
           s"${childAggregatorHost}:${childAggregatorPort}")
  }

  def writeToTable(tableName: String): Unit = {
    val df = spark.createDF(
      List(4, 5, 6),
      List(("id", IntegerType, true))
    )
    df.write
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .mode(SaveMode.Append)
      .save(s"${dbName}.${tableName}")
  }

  def readFromTable(tableName: String): DataFrame = {
    spark.read
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .load(s"${dbName}.${tableName}")
  }

  def writeAndReadFromTable(tableName: String): Unit = {
    writeToTable(tableName)
    val dataFrame = readFromTable(tableName)
    val sqlRows   = dataFrame.collect();
    assert(sqlRows.length == 3)
  }

  def dropTable(tableName: String): Unit =
    executeQueryWithLog(s"drop table if exists $dbName.$tableName")

  describe("Success during write operations") {

    it("to common table") {
      dropTable(commonCollectionName)
      executeQueryWithLog(
        s"create table if not exists $dbName.$commonCollectionName (id INT NOT NULL, PRIMARY KEY (id))")
      writeAndReadFromTable(commonCollectionName)
    }

    it("to reference table") {
      dropTable(referenceCollectionName)
      executeQueryWithLog(
        s"create reference table if not exists $dbName.$referenceCollectionName (id INT NOT NULL, PRIMARY KEY (id))")
      writeAndReadFromTable(referenceCollectionName)
    }
  }

  describe("Success during creating") {

    it("common table") {
      dropTable(commonCollectionName)
      writeAndReadFromTable(commonCollectionName)
    }
  }

  describe("Failure because of") {

    it("database name not specified") {
      spark.conf.set("spark.datasource.singlestore.database", "")
      val df = spark.createDF(
        List(4, 5, 6),
        List(("id", IntegerType, true))
      )
      val result = Try {
        df.write
          .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
          .mode(SaveMode.Append)
          .save(s"${commonCollectionName}")
      }
      /* Error code description:
        1046 = Database name not provided
       * */
      assert(TestHelper.isSQLExceptionWithCode(result.failed.get, List(1046)))
    }
  }

  it("clientEndpoint option") {
    if (version.atLeast("7.5.0")) {
      val cloudSpark = SparkSession
        .builder()
        .master("local")
        .appName("singlestore-integration-jwt-test")
        .config("spark.datasource.singlestore.clientEndpoint",
                s"${childAggregatorHost}:${childAggregatorPort}")
        .config("spark.datasource.singlestore.user", "root")
        .config("spark.datasource.singlestore.password", masterPassword)
        .config("spark.datasource.singlestore.database", "testdb")
        .getOrCreate()

      spark.executeSinglestoreQuery(
        s"create reference table if not exists $dbName.$referenceCollectionName (id INT NOT NULL, PRIMARY KEY (id))")

      val df = cloudSpark.createDF(
        List(4, 5, 6),
        List(("id", IntegerType, true))
      )
      df.write
        .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
        .mode(SaveMode.Append)
        .save(s"${dbName}.${referenceCollectionName}")
    }
  }
}
