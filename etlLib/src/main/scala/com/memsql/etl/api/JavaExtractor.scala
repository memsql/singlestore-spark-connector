package com.memsql.etl.api

import org.apache.spark.streaming.api.java.{JavaStreamingContext, JavaInputDStream}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.StreamingContext

trait JavaExtractor[A] extends Serializable {
  def extract(ssc: JavaStreamingContext): JavaInputDStream[A]
}
