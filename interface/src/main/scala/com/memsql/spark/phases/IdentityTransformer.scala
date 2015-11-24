package com.memsql.spark.phases

import com.memsql.spark.etl.api.{Transformer, PhaseConfig}
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.sql.{DataFrame, SQLContext}

// A pass though Transformer
class IdentityTransformer extends Transformer {
  override def transform(sqlContext: SQLContext, df: DataFrame, config: PhaseConfig, logger: PhaseLogger): DataFrame = {
    df
  }
}
