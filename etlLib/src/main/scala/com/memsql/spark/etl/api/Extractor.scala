package com.memsql.spark.etl.api

import java.nio.ByteBuffer

import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.StreamingContext
import com.memsql.spark.etl.api.configs.PhaseConfig

trait Extractor[S] extends Serializable {
  def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long): InputDStream[S]
}

trait ByteArrayExtractor extends Extractor[Array[Byte]] {
  def stringToBytes(x: String): Array[Byte] = x.getBytes
  def longToBytes(x: Long): Array[Byte] = ByteBuffer.allocate(java.lang.Long.SIZE / java.lang.Byte.SIZE).putLong(x).array
}
