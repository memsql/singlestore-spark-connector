package com.memsql.etl.api

import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.StreamingContext

trait Extractor[S] extends Serializable {
  def extract(ssc: StreamingContext): InputDStream[S]
}
