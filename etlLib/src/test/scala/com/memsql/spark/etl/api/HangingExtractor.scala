package com.memsql.spark.etl.api

import org.apache.log4j.Logger
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import com.memsql.spark.etl.api.configs.PhaseConfig

class HangingExtractor extends Extractor[String] {
  override def extract(ssc: StreamingContext, config: PhaseConfig, interval: Long, logger: Logger): InputDStream[String] = {
    while (true) {
      Thread.sleep(100000)
    }
    null
  }
}
