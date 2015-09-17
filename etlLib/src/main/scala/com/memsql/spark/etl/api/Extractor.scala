package com.memsql.spark.etl.api

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Time, StreamingContext}
import com.memsql.spark.etl.utils.{PhaseLogger, ByteUtils}

abstract class Extractor[S] extends Serializable {
  def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long, logger: PhaseLogger): InputDStream[S]
}

abstract class ByteArrayExtractor extends Extractor[Array[Byte]] {
  final var byteUtils = ByteUtils
}

abstract class SimpleByteArrayExtractor extends ByteArrayExtractor {
  final def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long, logger: PhaseLogger): InputDStream[Array[Byte]] = {
    val userConfig = extractConfig.asInstanceOf[UserExtractConfig]

    new InputDStream[Array[Byte]](ssc) {
      override def start(): Unit = SimpleByteArrayExtractor.this.initialize(ssc.sparkContext, userConfig, batchInterval, logger)

      override def stop(): Unit = SimpleByteArrayExtractor.this.cleanup(ssc.sparkContext, userConfig, batchInterval, logger)

      override def compute(validTime: Time): Option[RDD[Array[Byte]]] = SimpleByteArrayExtractor.this.nextRDD(ssc.sparkContext, userConfig, batchInterval, logger)
    }
  }

  def initialize(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Unit = {}

  def cleanup(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Unit = {}

  def nextRDD(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Option[RDD[Array[Byte]]]
}
