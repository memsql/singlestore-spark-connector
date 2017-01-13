// scalastyle:off regex

package com.memsql.spark.connector.sql

import java.util.Calendar

import com.memsql.spark.connector.{MemSQLConf, _}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.scalatest.{FlatSpec, Matchers}

class ExtraColumnsSpec extends FlatSpec with SharedMemSQLContext with Matchers {

   /* If we specify a list of extra columns in the SaveToMemSQLConf passed to
      saveToMemSQL(), the extra columns included in the MemSQL table that we create.
      This only works if the table does not already exist.
    */
  "Saving a dataframe to MemSQL with extraColumns parameter in SaveToMemSQLConf" should "populate the extra columns with default values" in {
    val rdd = sc.parallelize(Array(Row(1)))
    val schema = StructType(
      Array(
        StructField("a", IntegerType, true)
      )
    )
    val df = ss.createDataFrame(rdd, schema)

    val extraColumns = List(
      ColumnDefinition("b", "integer", false, defaultSQL = Some("42")),
      ColumnDefinition("c", "timestamp", false, defaultSQL = Some("CURRENT_TIMESTAMP"))
    )
    val saveConf = SaveToMemSQLConf(
      MemSQLConf.DEFAULT_SAVE_MODE,
      MemSQLConf.DEFAULT_CREATE_MODE,
      None,
      MemSQLConf.DEFAULT_INSERT_BATCH_SIZE,
      MemSQLConf.DEFAULT_LOAD_DATA_COMPRESSION,
      false,
      extraColumns,
      Nil,
      false
    )

    val tableId = TableIdentifier(dbName, "t")
    df.saveToMemSQL(tableId, saveConf)

    /* Read the table back out as a dataframe and verify the extra columns were created */
    val schema1 = StructType(
      Array(
        StructField("a", IntegerType, true),
        StructField("b", IntegerType, true),
        StructField("c", TimestampType, true),
        StructField("memsql_insert_time", TimestampType, true)
      )
    )

    val df_t = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> tableId.toString))
      .load()

    df_t.schema shouldBe schema1
    df_t.count shouldBe 1
    df_t.head.getInt(0) shouldBe 1
    df_t.head.getInt(1) shouldBe 42

    val cValue = df_t.head.getTimestamp(2)
    val cal = Calendar.getInstance
    cal.setTime(cValue)
    val years = cal.get(Calendar.YEAR)
    years should be >= 2015
  }

  it should "not add the extra columns to an existing table" in {
    val firstRDD = sc.parallelize(Seq(Row(1, 0), Row(1, 0)))
    val firstSchema = StructType(
      Array(
        StructField("a", IntegerType, true),
        StructField("b", IntegerType, true)
      )
    )
    val firstDF = ss.createDataFrame(firstRDD, firstSchema)

    val firstSaveConf = SaveToMemSQLConf(
      MemSQLConf.DEFAULT_SAVE_MODE,
      MemSQLConf.DEFAULT_CREATE_MODE,
      None,
      MemSQLConf.DEFAULT_INSERT_BATCH_SIZE,
      MemSQLConf.DEFAULT_LOAD_DATA_COMPRESSION,
      false,
      Nil, /* ExtraColumns */
      Nil,
      false
    )

    val tableId = TableIdentifier(dbName, "t2")
    firstDF.saveToMemSQL(tableId, firstSaveConf)

    val secondRDD = sc.parallelize(Seq(Row(0, 1), Row(1, 0), Row(3, 1)))
    val secondSchema = StructType(
      Array(
        StructField("a", IntegerType, true),
        StructField("b", IntegerType, true)
      )
    )
    val secondDF = ss.createDataFrame(secondRDD, secondSchema)

    val extraColumns = List(
      ColumnDefinition("c", "integer", false, defaultSQL = Some("42")),
      ColumnDefinition("d", "timestamp", false, defaultSQL = Some("CURRENT_TIMESTAMP"))
    )
    val secondSaveConf = SaveToMemSQLConf(
      MemSQLConf.DEFAULT_SAVE_MODE,
      MemSQLConf.DEFAULT_CREATE_MODE,
      None,
      MemSQLConf.DEFAULT_INSERT_BATCH_SIZE,
      MemSQLConf.DEFAULT_LOAD_DATA_COMPRESSION,
      false,
      extraColumns,
      Nil,
      false
    )

    secondDF.saveToMemSQL(tableId, secondSaveConf)

    /* Read the table back out as a dataframe and verify the extra columns were not created */
    val df_t2 = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> tableId.toString))
      .load()

    val withNoExtraColumns = StructType(
      Array(
        StructField("a", IntegerType, true),
        StructField("b", IntegerType, true),
        StructField("memsql_insert_time", TimestampType, true)
      )
    )

    val withExtraColumns = StructType(
      Array(
        StructField("a", IntegerType, true),
        StructField("b", IntegerType, true),
        StructField("c", IntegerType, true),
        StructField("d", TimestampType, true),
        StructField("memsql_insert_time", TimestampType, true)
      )
    )

    df_t2.schema shouldBe withNoExtraColumns
    df_t2.schema should not be withExtraColumns

    /* It's a columnstore table by default with no uniqueness, so this will result in more rows being inserted */
    df_t2.count shouldBe 5
  }
}
