package com.memsql.spark

import java.sql.SQLSyntaxErrorException

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.memsql.spark.MemsqlOptions.{CompressionType, TableKeyType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.BeforeAndAfterEach

class SanityTest extends IntegrationSuiteBase with BeforeAndAfterEach {
  var df: DataFrame = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    df = spark.createDF(
      List((1, "Albert"), (5, "Ronny"), (7, "Ben"), (9, "David")),
      List(("id", IntegerType, true), ("name", StringType, true))
    )
    writeTable("testdb.foo", df)
  }

  it("sets strict sql session variables") {
    // set a large but not exactly the same sql select limit
    executeQueryWithLog( "set global sql_select_limit = 18446744000000000000")

    val variables = spark.read
      .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
      .load("information_schema.session_variables")
      .collect()
      .groupBy(r => r.get(0))
      .mapValues(r => r.map(_.getString(1)).head)

    assert(variables("COLLATION_SERVER") == "utf8_general_ci")
    assert(variables("SQL_SELECT_LIMIT") == "18446744073709551615")
    assert(variables("COMPILE_ONLY") == "OFF")

    val sql_mode = spark.read
      .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
      .option(MemsqlOptions.QUERY, "select @@sql_mode")
      .load()
      .collect()
      .head
      .getString(0)
    assert(sql_mode == "ONLY_FULL_GROUP_BY,STRICT_ALL_TABLES")
  }

  it("DataSource V1 read sanity") {
    val x = spark.read
      .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
      .option(MemsqlOptions.TABLE_NAME, "testdb.foo")
      .load()
      .withColumn("hello", lit(2))
      .filter(col("id") > 1)
      .limit(1000)
      .groupBy(col("id"))
      .agg(count("*"))

    assertSmallDataFrameEquality(
      x,
      df.withColumn("hello", lit(2))
        .filter(col("id") > 1)
        .limit(1000)
        .groupBy(col("id"))
        .agg(count("*")),
      orderedComparison = false
    )
  }

  it("DataSource V1 write sanity") {
    for (compression <- CompressionType.values) {

      for (truncate <- Seq(false, true)) {
        println(s"testing datasource with compression=$compression, truncate=$truncate")
        df.write
          .format(DefaultSource.MEMSQL_SOURCE_NAME)
          // toLowerCase to test case insensitivity
          .option(MemsqlOptions.LOAD_DATA_COMPRESSION, compression.toString.toLowerCase())
          .option(MemsqlOptions.TRUNCATE, truncate.toString.toLowerCase())
          .mode(SaveMode.Overwrite)
          .save("testdb.tb2")

        val x = spark.read
          .format("jdbc")
          .option("url", s"jdbc:mysql://$masterHost:$masterPort/testdb")
          .option("dbtable", "testdb.tb2")
          .option("user", "root")
          .load()

        assertSmallDataFrameEquality(x, df, true, true)
      }
    }
  }

  describe("DataSource V1 can create table with different kinds of keys") {
    def createTableWithKeys(keys: Map[String, String]) =
      df.write
        .format("memsql")
        .mode(SaveMode.Overwrite)
        .options(keys)
        .save("testdb.keytest")

    it("works for all key types") {
      for (keyType <- TableKeyType.values) {
        if (keyType != TableKeyType.Shard) {
          println(s"testing create table with keyType=$keyType")
          createTableWithKeys(
            Map(
              s"tableKey.shard"               -> "name",
              s"tableKey.${keyType.toString}" -> "name"
            ))
        }
      }
    }

    it("errors when the user specifies an invalid key type") {
      assertThrows[RuntimeException] {
        createTableWithKeys(Map(s"tableKey.foo.id" -> "id"))
      }
    }

    it("errors when the user specifies duplicate key names") {
      assertThrows[SQLSyntaxErrorException] {
        createTableWithKeys(
          Map(
            s"tableKey.key.foo"    -> "id",
            s"tableKey.unique.foo" -> "id"
          )
        )
      }
    }

    it("throws when no type and no name is specified") {
      assertThrows[RuntimeException] {
        // no type specified, no name specified
        createTableWithKeys(Map(s"tableKey.." -> "id"))
      }
    }

    it("throws when no type is specified") {
      assertThrows[RuntimeException] {
        // no type specified
        createTableWithKeys(Map(s"tableKey." -> "id"))
      }
    }

    it("throws when no name is specified") {
      assertThrows[RuntimeException] {
        // no type specified
        createTableWithKeys(Map(s"tableKey.key." -> "id"))
      }
    }

    it("supports multiple columns") {
      createTableWithKeys(Map(s"tableKey.primary"     -> "id, name"))
      createTableWithKeys(Map(s"tableKey.columnstore" -> "id, name"))
      createTableWithKeys(Map(s"tableKey.key"         -> "id, name"))
      createTableWithKeys(
        Map(
          s"tableKey.unique" -> "id, name",
          s"tableKey.shard"  -> "id, name"
        ))
    }

    it("supports spaces and dots in the key name") {
      createTableWithKeys(Map(s"tableKey.primary.foo.bar" -> "id"))
      createTableWithKeys(Map(s"tableKey.key.cool key"    -> "id"))
      createTableWithKeys(Map(s"tableKey.key...."         -> "id"))
    }
  }
}
