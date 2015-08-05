package com.memsql.spark.etl.api

import org.apache.spark.streaming.{Time, StreamingContext}
import org.apache.spark.sql.SQLContext

trait ETLPipeline[S]  {
  val extractor: Extractor[S]
  val transformer: Transformer[S]
  val loader: Loader
}
