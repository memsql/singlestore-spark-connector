package com.memsql.spark.etl.api

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import com.memsql.spark.etl.api.configs.PhaseConfig

class HangingTransformer extends Transformer[String] {
  override def transform(sqlContext: SQLContext, rdd: RDD[String], phaseConfig: PhaseConfig, logger: Logger): DataFrame = {
    while (true) {
      Thread.sleep(100000)
    }
    null
  }
}
