package com.memsql.spark.etl.api

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import com.memsql.spark.etl.api.configs.PhaseConfig

trait Transformer[S] extends Serializable {
  def transform(sqlContext: SQLContext, rdd: RDD[S], transformConfig: PhaseConfig): DataFrame
}
