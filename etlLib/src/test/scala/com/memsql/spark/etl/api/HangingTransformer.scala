package com.memsql.spark.etl.api

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import com.memsql.spark.etl.api.configs.PhaseConfig

class HangingTransformer extends Transformer[String] {
  def transform(sqlContext: SQLContext, rdd: RDD[String], phaseConfig: PhaseConfig): DataFrame = {
    while (true) {
      Thread.sleep(100000)
    }
    null
  }
}
