package com.memsql.spark.etl.api

import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.StreamingContext
import com.memsql.spark.etl.api.configs.PhaseConfig

trait Extractor[S] extends Serializable {
  def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long): InputDStream[S]
}

trait ByteArrayExtractor extends Extractor[Array[Byte]]
