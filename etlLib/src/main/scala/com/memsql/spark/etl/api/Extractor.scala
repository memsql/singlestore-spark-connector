package com.memsql.spark.etl.api

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Time, StreamingContext}
import com.memsql.spark.etl.api.configs.PhaseConfig
import com.memsql.spark.etl.utils.ByteUtils

abstract class Extractor[S] extends Serializable {
  def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long, logger: Logger): InputDStream[S]
}

abstract class ByteArrayExtractor extends Extractor[Array[Byte]] {
  final var byteUtils = ByteUtils
}

abstract class SimpleByteArrayExtractor extends ByteArrayExtractor {
  final def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long, logger: Logger): InputDStream[Array[Byte]] = {
    new InputDStream[Array[Byte]](ssc) {
      override def start(): Unit = SimpleByteArrayExtractor.this.initialize(ssc.sparkContext, extractConfig, batchInterval, logger)

      override def stop(): Unit = SimpleByteArrayExtractor.this.cleanup(ssc.sparkContext, extractConfig, batchInterval, logger)

      override def compute(validTime: Time): Option[RDD[Array[Byte]]] = SimpleByteArrayExtractor.this.nextRDD(ssc.sparkContext, extractConfig, batchInterval, logger)
    }
  }

  def initialize(sparkContext: SparkContext, config: PhaseConfig, batchInterval: Long, logger: Logger): Unit = {}

  def cleanup(sparkContext: SparkContext, config: PhaseConfig, batchInterval: Long, logger: Logger): Unit = {}

  def nextRDD(sparkContext: SparkContext, config: PhaseConfig, batchInterval: Long, logger: Logger): Option[RDD[Array[Byte]]]
}
