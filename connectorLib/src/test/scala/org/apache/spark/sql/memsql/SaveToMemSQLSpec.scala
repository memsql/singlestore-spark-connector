package org.apache.spark.sql.memsql

import com.memsql.spark.connector.sql.TableIdentifier
import org.apache.spark.sql.Row
import org.apache.spark.sql.memsql.test.SharedMemSQLContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.scalatest.{FlatSpec, Matchers}
import com.memsql.spark.connector._

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
    val df1 = msc.table("empty_rows_table")
    assert(df1.schema.equals(schema1))
    assert(df1.count == 1)
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
}
