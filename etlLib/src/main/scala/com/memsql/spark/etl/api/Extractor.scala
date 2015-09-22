package com.memsql.spark.etl.api

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Time, StreamingContext}
import com.memsql.spark.etl.utils.{PhaseLogger, ByteUtils}

/**
 * Pipeline Extractor interface.
 *
 * @tparam S type parameter of the generated [[org.apache.spark.streaming.dstream.InputDStream]].
 */
abstract class Extractor[S] extends Serializable {
  /**
   * Instantiates the [[org.apache.spark.streaming.dstream.InputDStream]] for this Extractor.
   *
   * @param ssc The StreamingContext that is used to run this pipeline.
   * @param extractConfig The Extractor configuration passed from MemSQL Ops.
   * @param batchInterval The batch interval passed from MemSQL Ops.
   * @param logger A logger instance that is integrated with MemSQL Ops.
   * @return The input source for your pipeline.
   */
  def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long, logger: PhaseLogger): InputDStream[S]
}

/**
 * Pipeline Extractor interface for byte arrays.
 *
 * Currently this is the only supported subclass of Extractor in MemSQL Streamliner.
 */
abstract class ByteArrayExtractor extends Extractor[Array[Byte]] {
  final var byteUtils = ByteUtils
}

/**
 * Convenience wrapper around ByteArrayExtractor for initialization, iteration, and cleanup of simple data sources.
 */
abstract class SimpleByteArrayExtractor extends ByteArrayExtractor {
  final override def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long, logger: PhaseLogger): InputDStream[Array[Byte]] = {
    val userConfig = extractConfig.asInstanceOf[UserExtractConfig]

    new InputDStream[Array[Byte]](ssc) {
      override def start(): Unit = SimpleByteArrayExtractor.this.initialize(ssc.sparkContext, userConfig, batchInterval, logger)

      override def stop(): Unit = SimpleByteArrayExtractor.this.cleanup(ssc.sparkContext, userConfig, batchInterval, logger)

      override def compute(validTime: Time): Option[RDD[Array[Byte]]] =
        SimpleByteArrayExtractor.this.nextRDD(ssc.sparkContext, userConfig, batchInterval, logger)
    }
  }

  /**
   * Initialization code for your Extractor.
   * This is called after instantiation of your Extractor and before [[nextRDD]].
   * The default implementation does nothing.
   *
   * @param sparkContext The SparkContext that is used to run this pipeline.
   * @param config The user defined configuration passed from MemSQL Ops.
   * @param batchInterval The batch interval passed from MemSQL Ops.
   * @param logger A logger instance that is integrated with MemSQL Ops.
   */
  def initialize(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Unit = {}

  /**
   * Cleanup code for your Extractor.
   * This is called after your pipeline has terminated.
   * The default implementation does nothing.
   *
   * @param sparkContext The SparkContext that is used to run this pipeline.
   * @param config The user defined configuration passed from MemSQL Ops.
   * @param batchInterval The batch interval passed from MemSQL Ops.
   * @param logger A logger instance that is integrated with MemSQL Ops.
   */
  def cleanup(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Unit = {}

  /**
   * Convenience method for generating simple [[org.apache.spark.rdd.RDD]]s from your Extractor.
   * This is called once per batch and the resulting [[org.apache.spark.rdd.RDD]] is passed
   * to the Transformer.
   *
   * @param sparkContext The SparkContext that is used to run this pipeline.
   * @param config The user defined configuration passed from MemSQL Ops.
   * @param batchInterval The batch interval passed from MemSQL Ops.
   * @param logger A logger instance that is integrated with MemSQL Ops.
   * @return An optional [[org.apache.spark.rdd.RDD]] with your extracted data. If it is not [[scala.None]],
   *         it will be passed through the rest of the pipeline.
   */
  def nextRDD(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Option[RDD[Array[Byte]]]
}
