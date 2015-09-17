package com.memsql.spark.etl.api

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import com.memsql.spark.etl.utils.PhaseLogger

class HangingExtractor extends Extractor[String] {
  override def extract(ssc: StreamingContext, config: PhaseConfig, interval: Long, logger: PhaseLogger): InputDStream[String] = {
    while (true) {
      Thread.sleep(100000)
    }
    null
  }
}
