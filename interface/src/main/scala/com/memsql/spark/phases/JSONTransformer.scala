package com.memsql.spark.phases

import com.memsql.spark.connector.dataframe.{JsonType, JsonValue}
import com.memsql.spark.etl.api.{StringTransformer, PhaseConfig}
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

case class JsonTransformConfig(column_name: String) extends PhaseConfig

// A Transformer which produces dataframes with a single StringType column containing the json data.
// Appropriate for working with Loaders which load into tables with a single (Parquette) JSON column
// and possibly a key column or columns with default/computed/timestamp columns
class JSONTransformer extends StringTransformer {
  var columnName: String = null

  override def initialize(sqlContext: SQLContext, transformConfig: PhaseConfig, logger: PhaseLogger): Unit = {
    val jsonTransformConfig = transformConfig.asInstanceOf[JsonTransformConfig]
    columnName = jsonTransformConfig.column_name
  }

  override def transform(sqlContext: SQLContext, rdd: RDD[String], transformConfig: PhaseConfig, logger: PhaseLogger): DataFrame = {
    val transformedRDD = rdd.map(r => Row(new JsonValue(r)))
    val schema = StructType(Array(StructField(columnName, JsonType, true)))
    sqlContext.createDataFrame(transformedRDD, schema)
  }
}
