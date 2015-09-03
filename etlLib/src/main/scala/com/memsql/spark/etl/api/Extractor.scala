package com.memsql.spark.etl.api

import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.StreamingContext
import com.memsql.spark.etl.api.configs.PhaseConfig

abstract class Extractor[S] extends Serializable {
  def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long, logger: Logger): InputDStream[S]
}

abstract class ByteArrayExtractor extends Extractor[Array[Byte]]
