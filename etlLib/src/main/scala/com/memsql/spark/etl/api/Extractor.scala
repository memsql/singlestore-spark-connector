package com.memsql.spark.etl.api

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Time, StreamingContext}
import com.memsql.spark.etl.utils.{PhaseLogger, ByteUtils}

/**
 * Thrown when an Extractor is checkpointed without overriding [[Extractor.batchCheckpoint]]
 * and [[Extractor.batchRetry)]]
 */
class ExtractorCheckpointException(message: String) extends Exception(message: String)

/**
 * Pipeline Extractor interface.
 */
abstract class Extractor extends Serializable {
  /**
   * Initialization code for your Extractor.
   * This is called after instantiation of your Extractor and before [[next]].
   * The default implementation does nothing.
   *
   * @param ssc The [[org.apache.spark.streaming.StreamingContext]] that is used to run this pipeline.
   * @param sqlContext The [[org.apache.spark.sql.SQLContext]] that is used to run this pipeline.
   * @param config The Extractor configuration passed from MemSQL Ops.
   * @param batchInterval The batch interval passed from MemSQL Ops.
   * @param logger A logger instance that is integrated with MemSQL Ops.
   */
  def initialize(ssc: StreamingContext, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
                 logger: PhaseLogger): Unit = {}

  /**
   * Cleanup code for your Extractor.
   * This is called after your pipeline has stopped.
   * The default implementation does nothing.
   *
   * @param ssc The [[org.apache.spark.streaming.StreamingContext]] that is used to run this pipeline.
   * @param sqlContext The [[org.apache.spark.sql.SQLContext]] that is used to run this pipeline.
   * @param config The Extractor configuration passed from MemSQL Ops.
   * @param batchInterval The batch interval passed from MemSQL Ops.
   * @param logger A logger instance that is integrated with MemSQL Ops.
   */
  def cleanup(ssc: StreamingContext, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
              logger: PhaseLogger): Unit = {}

  /**
   * Compute the next [[org.apache.spark.sql.DataFrame]] of extracted data.
   *
   * @param ssc The [[org.apache.spark.streaming.StreamingContext]] that is used to run this pipeline.
   * @param time The timestamp from which data is being extracted.
   * @param sqlContext The [[org.apache.spark.sql.SQLContext]] that is used to create [[org.apache.spark.sql.DataFrame]]s.
   * @param config The Extractor configuration passed from MemSQL Ops.
   * @param batchInterval The batch interval passed from MemSQL Ops.
   * @param logger A logger instance that is integrated with MemSQL Ops.
   * @return An optional [[org.apache.spark.sql.DataFrame]] with your extracted data. If it is not [[scala.None]],
   *         it will be passed through the rest of the pipeline.
   */
  def next(ssc: StreamingContext, time: Long, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
           logger: PhaseLogger): Option[DataFrame] = None

  /**
   * The last checkpoint that this extractor saved. Value is [[scala.None]] if there is no checkpoint data or it
   * could not be deserialized.
   */
  final var lastCheckpoint: Option[Map[String, Any]] = None

  /**
   * Called at the end of a pipeline batch if it has succeeded. Override this and [[batchRetry]] to implement
   * checkpointing for your Extractor. The default implementation will throw a [[ExtractorCheckpointException]]
   * if the Extractor is run with checkpointing enabled.
   *
   * @return The serialized data to be saved in the checkpoint database for the successful batch.
   */
  def batchCheckpoint(): Option[Map[String, Any]] = {
    throw new ExtractorCheckpointException("In order to use checkpointing on this extractor, it must override batchCheckpoint.")
  }

  /**
   * Called at the end of a pipeline batch if it has failed. Override this and [[batchCheckpoint]] to implement
   * checkpointing for your Extractor. The default implementation will throw a [[ExtractorCheckpointException]]
   * if the Extractor is run with checkpointing enabled.
   *
   * Use this to reset the Extractor state so it will retry the failed batch on the next call to [[next]].
   */
  def batchRetry(): Unit = {
    throw new ExtractorCheckpointException("In order to use checkpointing on this extractor, it must override batchRetry.")
  }

  /**
    * Check if this extractor has overridden both [[batchCheckpoint]] and [[batchRetry]].
    * This is used to determine if the Spark interface will enable checkpointing support for pipelines which use this
    * extractor.
    */
  def usesCheckpointing(): Boolean = {
    try {
      val batchCheckpointClass = this.getClass.getMethod("batchCheckpoint").getDeclaringClass()
      val batchRetryClass = this.getClass.getMethod("batchRetry").getDeclaringClass()
      batchCheckpointClass != classOf[Extractor] && batchRetryClass != classOf[Extractor]
    } catch {
      case e: Exception => false // ignore errors here
    }
  }
}

/**
 * Pipeline Extractor interface for byte arrays.
 */
@deprecated("Extractor interface supports DataFrames", "1.2.0")
abstract class ByteArrayExtractor extends Extractor {
  val byteUtils = ByteUtils
  private var dStream: InputDStream[Array[Byte]] = null

  /**
   * The schema used to turn the emitted [[org.apache.spark.rdd.RDD]]s into [[org.apache.spark.sql.DataFrame]]
   */
  def schema: StructType = StructType(StructField("bytes", BinaryType, false) :: Nil)

  /**
   * Instantiates the [[org.apache.spark.streaming.dstream.InputDStream]] for this Extractor.
   *
   * @param ssc The StreamingContext that is used to run this pipeline.
   * @param extractConfig The Extractor configuration passed from MemSQL Ops.
   * @param batchInterval The batch interval passed from MemSQL Ops.
   * @param logger A logger instance that is integrated with MemSQL Ops.
   * @return The input source for your pipeline. The [[org.apache.spark.rdd.RDD]]s that are returned will automatically
   *         be turned into [[org.apache.spark.sql.DataFrame]]s with the defined [[schema]].
   */
  def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long, logger: PhaseLogger): InputDStream[Array[Byte]]

  override def next(ssc: StreamingContext, time: Long, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
                    logger: PhaseLogger): Option[DataFrame] = {
    if (dStream == null) {
      dStream = extract(ssc, config, batchInterval, logger)
    }

    dStream.compute(Time(time)).map(rdd => {
      val rddRow = rdd.map(Row(_))
      sqlContext.createDataFrame(rddRow, schema)
    })
  }
}

/**
 * Convenience wrapper around ByteArrayExtractor for initialization, iteration, and cleanup of simple data sources.
 */
@deprecated("Extractor interface supports DataFrames", "1.2.0")
abstract class SimpleByteArrayExtractor extends ByteArrayExtractor {
  final override def initialize(ssc: StreamingContext, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
                                logger: PhaseLogger): Unit = {
    initialize(sqlContext.sparkContext, config.asInstanceOf[UserExtractConfig], batchInterval, logger)
  }

  final override def cleanup(ssc: StreamingContext, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
                             logger: PhaseLogger): Unit = {
    cleanup(sqlContext.sparkContext, config.asInstanceOf[UserExtractConfig], batchInterval, logger)
  }

  final override def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long,
                             logger: PhaseLogger): InputDStream[Array[Byte]] = {
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
   * This is called after your pipeline has stopped.
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
