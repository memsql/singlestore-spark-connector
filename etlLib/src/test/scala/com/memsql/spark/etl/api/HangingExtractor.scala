package com.memsql.spark.etl.api

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import com.memsql.spark.etl.api.configs.PhaseConfig

class HangingExtractor extends Extractor[String] {
  def extract(ssc: StreamingContext, config: PhaseConfig, interval: Long): InputDStream[String] = {
    while (true) {
      Thread.sleep(100000)
    }
    null
  }
}
