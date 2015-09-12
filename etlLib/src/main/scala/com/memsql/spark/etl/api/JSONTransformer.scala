package com.memsql.spark.etl.api

import com.memsql.spark.etl.api.configs.{JsonTransformConfig, PhaseConfig}
import com.memsql.spark.connector.dataframe.{JsonType, JsonValue}
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

// A Transformer which produces dataframes with a single StringType column containing the json data.
// Appropriate for working with Loaders which load into tables with a single (Parquette) JSON column and possibly a key column or columns with default/computed/timestamp columns
//
class JSONTransformer extends ByteArrayTransformer {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], transformConfig: PhaseConfig, logger: PhaseLogger): DataFrame = {
    val jsonTransformConfig = transformConfig.asInstanceOf[JsonTransformConfig]
    val transformedRDD = rdd.map(r => Row(new JsonValue(byteUtils.bytesToUTF8String(r))))
    val schema = StructType(Array(StructField(jsonTransformConfig.column_name, JsonType, true)))
    sqlContext.createDataFrame(transformedRDD, schema)
  }
}
