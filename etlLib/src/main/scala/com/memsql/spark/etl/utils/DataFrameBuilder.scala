package com.memsql.spark.etl.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.SparkException

/**
 * A DataFrameBuilder is a utility for creating DataFrames in a mistake-resistant way from the executors.
 * The schema is specified up front, and the row producing code sets values in its row.
 * Unset columns are left as null.
 * Attempting to set a column not found in the schema will throw a SparkException.
 *
 * Example:
 *     val dfb: = DataFrameBuilder(StructType(Array(StructField("a",StringType,true),StructField("b",IntegerType,true))))
 *     val rowRDD = rdd.map { r =>
 *       val rb = dfb.startRow
 *       ...
 *       rb.set("a",...)
 *         .set("b",...)
 *         .finishRow
 *     }
 *     dfb.finishDataFrame(sqlContext, rowRDD)
 *
 * see also: `MemSQLContext::getTableSchema`
 */
case class DataFrameBuilder(schema: StructType) extends Serializable {
  val colNames: Array[String] = schema.map((col: StructField) => col.name).toArray
  def startRow: RowBuilder = RowBuilder(colNames)
  def finishDataFrame(sqlContext: SQLContext, rdd: RDD[Row]): DataFrame = sqlContext.createDataFrame(rdd, schema)
}

case class RowBuilder(colNames: Array[String]) {
  private val row: Array[Any] = colNames.map((x:String) => null)
  def set(name: String, value: Any): RowBuilder = {
    val ix = colNames.indexOf(name)
    if (ix == -1) {
      throw new SparkException("Tried to set row " + name + ", which does not exist")
    }
    row(ix) = value
    this
  }
  def finishRow: Row = Row.fromSeq(row)
}
