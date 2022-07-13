package com.singlestore.spark

import java.sql.SQLSyntaxErrorException

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.singlestore.spark.SinglestoreOptions.{CompressionType, TableKeyType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterEach
import com.singlestore.spark.SQLHelper._

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
    executeQueryWithLog("set global sql_select_limit = 18446744000000000000")

    val variables = spark.read
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .load("information_schema.session_variables")
      .collect()
      .groupBy(r => r.get(0))
      .mapValues(r => r.map(_.getString(1)).head)

    assert(variables("COLLATION_SERVER") == "utf8_general_ci")
    assert(variables("SQL_SELECT_LIMIT") == "18446744073709551615")
    assert(variables("COMPILE_ONLY") == "OFF")

    val sql_mode = spark.read
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .option(SinglestoreOptions.QUERY, "select @@sql_mode")
      .load()
      .collect()
      .head
      .getString(0)
    assert(sql_mode == "ONLY_FULL_GROUP_BY,STRICT_ALL_TABLES")
  }

  it("DataSource V1 read sanity") {
    val x = spark.read
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .option(SinglestoreOptions.TABLE_NAME, "testdb.foo")
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
          .format(DefaultSource.SINGLESTORE_SOURCE_NAME)
          // toLowerCase to test case insensitivity
          .option(SinglestoreOptions.LOAD_DATA_COMPRESSION, compression.toString.toLowerCase())
          .option(SinglestoreOptions.TRUNCATE, truncate.toString.toLowerCase())
          .mode(SaveMode.Overwrite)
          .save("testdb.tb2")

        val x = spark.read
          .format("jdbc")
          .option("url", s"jdbc:mysql://$masterHost:$masterPort/testdb")
          .option("dbtable", "testdb.tb2")
          .option("user", "root")
          .option("password", masterPassword)
          .load()

        assertSmallDataFrameEquality(x,
                                     df,
                                     true,
                                     true,
                                     orderedComparison = !canDoParallelReadFromAggregators)
      }
    }
  }

  describe("DataSource V1 can create table with different kinds of keys") {
    def createTableWithKeys(keys: Map[String, String]) =
      df.write
        .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
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

  def repartitionColumnsTest(): Unit = {
    val expectedDf = spark.createDF(
      List((1, 1, "Albert", "1"), (5, 2, "Ronny", "2"), (7, 3, "Ben", "4"), (9, 4, "David", "5")),
      List(("id", IntegerType, true),
           ("id2", IntegerType, true),
           (",a,,", StringType, true),
           (" ", StringType, true))
    )
    writeTable("testdb.foo", expectedDf)

    spark.sqlContext.setConf("spark.datasource.singlestore.enableParallelRead", "forced")
    val actualDf = spark.read
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .option("parallelRead.repartition", "true")
      .option("parallelRead.repartition.columns", "id,   `,a,,` ,   ` `")
      .load("testdb.foo")

    assertSmallDataFrameEquality(actualDf, expectedDf, orderedComparison = false)
    spark.sqlContext.setConf("spark.datasource.singlestore.enableParallelRead", "automaticLite")
  }

  it("repartition columns pushdown enabled") {
    spark.sqlContext.setConf("spark.datasource.singlestore.disablePushdown", "false")
    repartitionColumnsTest()
  }

  it("repartition columns pushdown disabled") {
    spark.sqlContext.setConf("spark.datasource.singlestore.disablePushdown", "true")
    repartitionColumnsTest()
    spark.sqlContext.setConf("spark.datasource.singlestore.disablePushdown", "false")
  }

  it("creates rowstore table") {
    df.write
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .option("createRowstoreTable", "true")
      .save("testdb.rowstore")

    val rows = spark.executeSinglestoreQueryDB(
      "testdb",
      "select storage_type from information_schema.tables where table_schema='testdb' and table_name='rowstore';")
    assert(rows.size == 1, "Only one row should be selected")
    rows.foreach(row =>
      assert(row.getString(0).equals("INMEMORY_ROWSTORE"), "Should create rowstore table"))
  }

  describe("parallel read num partitions") {

    def testNumPartitions(feature: String, numPartitions: Int, expectedNumPartitions: Int): Unit = {
      val initialConf = spark.conf.getAll
      spark.conf.set("spark.datasource.singlestore.parallelRead.Features", feature)
      spark.conf.set("spark.datasource.singlestore.enableParallelRead", "forced")

      try {
        val df = spark.read
          .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
          .option("enableParallelRead", "forced")
          .option("parallelRead.maxNumPartitions", numPartitions.toString)
          .load("testdb.foo")
          .cache()

        assert(df.rdd.getNumPartitions == expectedNumPartitions,
               "Wrong number of partition in the resulting RDD")
      } finally {
        val finalConf = spark.conf.getAll
        finalConf.foreach(c =>
          if (c._1.startsWith("spark.datasource.singlestore")) {
            spark.conf.unset(c._1)
        })
        initialConf.foreach(c =>
          if (c._1.startsWith("spark.datasource.singlestore")) {
            spark.conf.set(c._1, c._2)
        })
      }
    }

    for (feature <- Seq("readFromLeaves", "readFromAggregators", "readFromAggregatorsMaterialized")) {
      if (canDoParallelReadFromAggregators || feature == "readFromLeaves") {
        describe(feature) {
          it("default") {
            testNumPartitions(feature, 0, 2)
          }
          it("single partition") {
            testNumPartitions(feature, 1, 1)
          }
          it("a lot of partitions") {
            testNumPartitions(feature, 5, 2)
          }
        }
      }
    }
  }

  it("JWT authentication") {
    val jwtSpark = SparkSession
      .builder()
      .master("local")
      .appName("singlestore-integration-jwt-test")
      .config(
        "spark.datasource.singlestore.ddlEndpoint",
        s"${masterHost}:${masterPort}"
      )
      .config("spark.datasource.singlestore.user", "test_jwt_user")
      .config(
        "spark.datasource.singlestore.password",
        masterJWTPassword
      )
      .config("spark.datasource.singlestore.database", "testdb")
      .config("spark.datasource.singlestore.credentialType", "JWT")
      .getOrCreate()

    // Read with enabled sslMode
    val jwtDF = jwtSpark.read
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .option("sslMode", "trust")
      .option(SinglestoreOptions.TABLE_NAME, "testdb.foo")
      .load()
    assertSmallDataFrameEquality(jwtDF, df, orderedComparison = false)
  }

  it("clientEndpoint option") {
    val cloudSpark = spark.newSession()

    cloudSpark.conf.unset("spark.datasource.singlestore.ddlEndpoint")
    cloudSpark.conf.unset("spark.datasource.singlestore.dmlEndpoint")
    cloudSpark.conf.set("spark.datasource.singlestore.clientEndpoint",
                        s"${masterHost}:${masterPort}")

    val jwtDF = cloudSpark.read
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .option(SinglestoreOptions.TABLE_NAME, "testdb.foo")
      .load()
    assertSmallDataFrameEquality(jwtDF, df, orderedComparison = false)
  }
}
