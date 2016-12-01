// scalastyle:off magic.number

package com.memsql.spark.connector.sql

import com.memsql.spark.SaveToMemSQLException
import com.memsql.spark.connector._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.FlatSpec


class TestSaveToMemSQLErrorsSpec extends FlatSpec with SharedMemSQLContext{
  "TestSaveToMemSQLErrors" should "save to MemSQL" in {
    val rdd1 = sc.parallelize(
      Array(Row(1, "test 1"),
        Row(2, "test 2"),
        Row(3, "test 3\ntest 4"),
        Row(4, "test 5\ttest 6")))
    val schema1 = StructType(
      Array(
        StructField("a", IntegerType, true),
        StructField("b", StringType, true)))
    val df1 = ss.createDataFrame(rdd1, schema1)
    df1.saveToMemSQL(dbName, "errors_table")

    val rdd2 = sc.parallelize(
      Array(Row(1, "test 1", "test 2")))
    val schema2 = StructType(
      Array(
        StructField("a", IntegerType, true),
        StructField("b", StringType, true),
        StructField("c", StringType, true)))
    val df2 = ss.createDataFrame(rdd2, schema2)

    val saveConf1 = SaveToMemSQLConf(
      ss.memSQLConf, Some(SaveMode.Append))
    try {
      // Trying to save df2 to errors_table should fail because errors_table
      // does not have a "c" colummn.
      df2.saveToMemSQL(TableIdentifier(dbName, "errors_table"), saveConf1)
      assert(false)
    } catch {
      case e: SaveToMemSQLException => {
        assert(e.exception.getMessage.contains("Unknown column 'c' in 'field list'"))
      }
    }

    val saveConf2 = SaveToMemSQLConf(
      ss.memSQLConf, Some(SaveMode.Append),
      Map("onDuplicateKeySQL" -> "c = 1"))
    try {
      // We should not be able to specify ON DUPLICATE KEY UPDATE c = 1 for
      // errors_table since errors_table doesn't have a c column.
      df1.saveToMemSQL(TableIdentifier(dbName, "errors_table"), saveConf2)
      assert(false)
    } catch {
      case e: SaveToMemSQLException => {
        assert(e.exception.getMessage.contains("Unknown column 'c' in 'field list'"))
      }
    }

    val saveConf3 = SaveToMemSQLConf(
      ss.memSQLConf, Some(SaveMode.Append),
      Map("createMode" -> "Skip"))
    try {
      // We should not be able to save to a nonexistent table if createMode is
      // skip.
      df1.saveToMemSQL(TableIdentifier(dbName, "nonexistent_table"), saveConf3)
      assert(false)
    } catch {
      case e: SaveToMemSQLException => {
        assert(e.exception.getMessage.contains(s"Table '${dbName}.nonexistent_table' doesn't exist"))
      }
    }

    try {
      val saveConf4 = SaveToMemSQLConf(
        ss.memSQLConf, Some(SaveMode.Append),
        Map("onDuplicateKeySQL" -> "b = 1", "createMode" -> "Skip"))
      // If we're inserting rows with INSERT (e.g. because we're using
      // onDuplicateKeySQL), we shouldn't allow duplicate column names in the
      // DataFrames we're saving.
      df1.select(df1("a"), df1("b"), df1("a")).saveToMemSQL(TableIdentifier(dbName, "errors_table"), saveConf4)
    } catch {
      case e: SaveToMemSQLException => {
        assert(e.exception.getMessage.contains("Column 'a' specified twice"))
      }
    }
  }
}
