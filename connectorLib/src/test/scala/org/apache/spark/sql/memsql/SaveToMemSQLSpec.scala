// scalastyle:off magic.number regex

package org.apache.spark.sql.memsql

import com.memsql.spark.SaveToMemSQLException
import com.memsql.spark.connector._
import com.memsql.spark.connector.dataframe.{JsonType, JsonValue}
import com.memsql.spark.connector.sql.{PrimaryKey, TableIdentifier}
import org.apache.spark.sql.memsql.test.{SharedMemSQLContext, TestUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.{FlatSpec, Matchers}

class SaveToMemSQLSpec extends FlatSpec with SharedMemSQLContext with Matchers {

  "saveToMemSQL" should "support saving empty rows" in {
    val rdd = sc.parallelize(Array(Row()))
    val schema = StructType(Array[StructField]())
    val df = msc.createDataFrame(rdd, schema)
    df.saveToMemSQL(dbName, "empty_rows_table")

    val schema1 = StructType(
      Array(
        StructField("memsql_insert_time", TimestampType, true)
      )
    )
    val df_t = msc.table("empty_rows_table")
    assert(df_t.schema.equals(schema1))
    assert(df_t.count == 1)
  }

  it should "support dry run" in {
    assert(msc.maybeTable("testDryRun").isEmpty)

    val tableIdent = TableIdentifier("testDryRun")
    val saveConf = SaveToMemSQLConf(msc.memSQLConf, params=Map("dryRun" -> "true"))

    val rdd = sc.parallelize(Array(Row(1)))
    val schema = StructType(Seq(StructField("num", IntegerType, true)))
    val df = msc.createDataFrame(rdd, schema)

    df.saveToMemSQL(tableIdent, saveConf)

    // the database and table should exist, but the table should be empty
    assert(msc.maybeTable("testDryRun").isDefined)
    assert(msc.table("testDryRun").count() == 0)
  }

  "saveToMemSQL" should "save a simple table" in {
    val rdd = sc.parallelize(
      Array(
        Row(1,    "pieguy"),
        Row(2,    "gbop"),
        Row(3,    "berry\ndave"),
        Row(4,    "psy\tduck"),
        Row(null, "null"),
        Row(6,    "berry\\tdave"),
        Row(7,    "berry\\ndave"),
        Row(8,    "\"berry\" 'dave'")))

    val schema = StructType(
      Array(
        StructField("a", IntegerType, true),
        StructField("b", StringType, true)))

    val df1 = sqlContext.createDataFrame(rdd, schema)

    df1.saveToMemSQL("t1")

    val df_t = msc.table("t1").drop("memsql_insert_time")
    df_t.schema shouldBe df1.schema
    df_t.count shouldBe 8
    TestUtils.equalDFs(df_t, df1) shouldBe true

    df1.saveToMemSQL("t1")
    TestUtils.equalDFs(df_t, df1.unionAll(df1)) shouldBe true

    df1.select("b", "a").saveToMemSQL("t1")
    TestUtils.equalDFs(df_t, df1.unionAll(df1).unionAll(df1)) shouldBe true

    val df2 = df1.where(df1("a") < 5).select(df1("a") + 1 as "b",df1("a"))
    df2.saveToMemSQL("t1")
    df_t.filter(df_t("b") === "3").count shouldBe 1
  }

  "saveToMemSQL" should "handle duplicate keys" in {
    val tableId = TableIdentifier(dbName, "t4")

    val schema = StructType(
      Array(
        StructField("a", IntegerType, true),
        StructField("b", StringType, true)))

    // Test 1: Regular insert

    val rdd1 = sc.parallelize(
      Array(
        Row(1, "test 1"),
        Row(2, "test 2"),
        Row(3, "test 3")))

    val df1 = msc.createDataFrame(rdd1, schema)

    val saveConf1 = TestUtils.getTestSaveConf(
      extraKeys = Seq(PrimaryKey("a"))
    )
    df1.saveToMemSQL(tableId, saveConf1)

    val df_t = msc.table("t4").drop("memsql_insert_time")
    df_t.schema shouldBe schema
    df_t.count shouldBe 3
    TestUtils.equalDFs(df_t, df1) shouldBe true

    // Test 2: Overwrite

    val rdd2 = sc.parallelize(
      Array(
        Row(1, "test 4"),
        Row(2, "test 5"),
        Row(3, "test 6")))

    val df2 = msc.createDataFrame(rdd2, schema)

    val saveConf2 = TestUtils.getTestSaveConf(
      saveMode = SaveMode.Overwrite
    )
    df2.saveToMemSQL(tableId, saveConf2)

    df_t.count shouldBe 3
    TestUtils.equalDFs(df_t, df2) shouldBe true

    // Test 3: Ignore

    val rdd3 = sc.parallelize(
      Array(
        Row(1, "test 7"),
        Row(2, "test 8"),
        Row(3, "test 9")))

    val df3 = msc.createDataFrame(rdd3, schema)

    val saveConf3a = TestUtils.getTestSaveConf(
      saveMode = SaveMode.Ignore
    )
    df3.saveToMemSQL(tableId, saveConf3a)

    df_t.count shouldBe 3
    TestUtils.equalDFs(df_t, df2) shouldBe true

    // Test 4: Append

    val saveConf3b = TestUtils.getTestSaveConf(
      saveMode = SaveMode.Append,
      onDuplicateKeySQL = Some("b = 'foobar'")
    )
    df3.saveToMemSQL(tableId, saveConf3b)

    val rdd4 = sc.parallelize(
      Array(Row(1, "foobar"),
            Row(2, "foobar"),
            Row(3, "foobar")))

    val df4 = msc.createDataFrame(rdd4, schema)
    df_t.count shouldBe 3
    TestUtils.equalDFs(df_t, df4) shouldBe true
  }

  "saveToMemSQL" should "throw a SaveToMemSQLException on RDD error" in {
    val rdd = sc.parallelize(
      Array(Row(1,"pieguy")))
      .map(x => {
        throw new Exception("Test exception 123")
        x
      })

    val schema = StructType(
      Array(
        StructField("a", IntegerType, true),
        StructField("b", StringType, true)
      )
    )
    val df = msc.createDataFrame(rdd, schema)

    a[SaveToMemSQLException] should be thrownBy {
      df.saveToMemSQL(dbName, "exception_table")
    }
  }

  "saveToMemSQL" should "properly create a table with JSON columns" in {
    val rdd = sc.parallelize(
      Array(
        Row(1, "[]"),
        Row(2, "\"foobar\"")
      )
    )

    val schema = StructType(
      Array(
        StructField("id", IntegerType, false),
        StructField("data", JsonType, false)
      )
    )
    val df = msc.createDataFrame(rdd, schema)
    df.saveToMemSQL(dbName, "json_test")

    val rdd2 = sc.parallelize(
      Array(
        Row(1, new JsonValue("[]")),
        Row(2, new JsonValue("\"foobar\""))
      )
    )

    val schema2 = StructType(
      Array(
        StructField("id", IntegerType, false),
        StructField("data", JsonType, true)
      )
    )
    val df2 = msc.createDataFrame(rdd2, schema2)
    df2.saveToMemSQL(dbName, "json_test_nullable")
  }
}
