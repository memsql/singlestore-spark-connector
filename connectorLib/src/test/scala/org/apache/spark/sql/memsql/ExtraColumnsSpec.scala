// scalastyle:off regex

package org.apache.spark.sql.memsql

import java.util.Calendar

import com.memsql.spark.connector.MemSQLConf
import com.memsql.spark.connector.sql.{ColumnDefinition, TableIdentifier}
import org.apache.spark.sql.Row
import org.apache.spark.sql.memsql.test.SharedMemSQLContext
import org.apache.spark.sql.types.{TimestampType, StructField, IntegerType, StructType}
import org.scalatest.{Matchers, FlatSpec}
import org.apache.spark.sql.memsql.SparkImplicits._

class ExtraColumnsSpec extends FlatSpec with SharedMemSQLContext with Matchers {
  it should "add extra columns to tables with working 'default SQL'" in {
    val rdd = sc.parallelize(Array(Row(1)))
    val schema = StructType(
      Array(
        StructField("a", IntegerType, true)
      )
    )
    val df = msc.createDataFrame(rdd, schema)

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

    val schema1 = StructType(
      Array(
        StructField("a", IntegerType, true),
        StructField("b", IntegerType, true),
        StructField("c", TimestampType, true),
        StructField("memsql_insert_time", TimestampType, true)
      )
    )
    val df_t = msc.table("t")

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
}
