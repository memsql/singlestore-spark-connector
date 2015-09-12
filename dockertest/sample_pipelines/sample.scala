package com.memsql.streamliner.starter

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.memsql.spark.context._
import com.memsql.spark.connector._
import com.memsql.spark.etl.api._
import com.memsql.spark.etl.api.configs._
import com.memsql.spark.etl.utils._
import com.memsql.spark.etl.utils.PhaseLogger

abstract class MyExtractor extends ByteArrayExtractor
abstract class MySimpleExtractor extends SimpleByteArrayExtractor

// The simplest implementation of an Extractor just provides a nextRDD method. This is useful for prototyping and debugging.
class ConstantExtractor extends MySimpleExtractor {
  override def nextRDD(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Option[RDD[Array[Byte]]] = {
    logger.info("emitting a constant RDD")

    Some(sparkContext.parallelize(List(1,2,3,4,5).map(byteUtils.intToBytes)))
  }
}

// An Extractor can also be configured with the config blob that is provided in MemSQL Ops.
class ConfigurableConstantExtractor extends MySimpleExtractor {
  override def nextRDD(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Option[RDD[Array[Byte]]] = {
    logger.info("emitting a constant RDD")

    val start = config.getConfigInt("start").getOrElse(1)
    val end = config.getConfigInt("end").getOrElse(5)
    Some(sparkContext.parallelize(List.range(start, end).map(byteUtils.intToBytes)))
  }
}

// A more complex Extractor which maintains some state can be implemented using the initialize and cleanup methods.
class SequenceExtractor extends MySimpleExtractor {
  var i: Int = Int.MinValue

  override def initialize(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Unit = {
    i = config.getConfigInt("sequence", "initial_value").getOrElse(0)

    logger.info(s"initializing the sequence at $i")
  }

  override def cleanup(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Unit = {
    logger.info("cleaning up the sequence")
  }

  override def nextRDD(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Option[RDD[Array[Byte]]] = {
    val sequenceSize = config.getConfigInt("sequence", "size").getOrElse(5)
    logger.info(s"emitting a sequence RDD from $i to ${i + sequenceSize}")

    i += sequenceSize
    Some(sparkContext.parallelize(List.range(i - sequenceSize, i).map(byteUtils.intToBytes)))
  }
}

// Finally, an Extractor can be implemented using any existing InputDStream that works with Spark Streaming.
class DStreamExtractor extends MyExtractor {
  override def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long, logger: PhaseLogger): InputDStream[Array[Byte]] = {
    logger.info("creating extractor from an InputDStream")

    new InputDStream[Array[Byte]](ssc) {
      override def start(): Unit = {}
      override def stop(): Unit = {}
      override def compute(validTime: Time): Option[RDD[Array[Byte]]] = Some(ssc.sparkContext.parallelize(List(0).map(byteUtils.intToBytes)))
    }
  }
}

abstract class MySimpleTransformer extends SimpleByteArrayTransformer

// A Transformer implements the transform method which turns an RDD into a DataFrame.
class EvenNumbersOnlyTransformer extends MySimpleTransformer {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: UserTransformConfig, logger: PhaseLogger): DataFrame = {
    logger.info("transforming the RDD")

    // transform the RDD into RDD[Row]
    val integerRDD = rdd.map(byteUtils.bytesToInt)
    val filteredRDD = integerRDD.filter(x => x % 2 == 0)
    val transformedRDD = filteredRDD.map(x => Row(x))

    // create a schema with a single non-nullable integer column named number
    val schema = StructType(Array(StructField("number", IntegerType, true)))

    sqlContext.createDataFrame(transformedRDD, schema)
  }
}

// A Transformer can also be configured with the config blob that is provided in MemSQL Ops.
class ConfigurableNumberParityTransformer extends MySimpleTransformer {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: UserTransformConfig, logger: PhaseLogger): DataFrame = {
    var keepEvenNumbers = config.getConfigBoolean("filter", "even").getOrElse(true)
    var keepOddNumbers = config.getConfigBoolean("filter", "odd").getOrElse(true)
    var columnName = config.getConfigString("table", "column_name").getOrElse("number")

    logger.info("transforming the RDD")

    // transform the RDD into RDD[Row] using the configured filter
    val integerRDD = rdd.map(byteUtils.bytesToInt)
    val filteredRDD = integerRDD.filter(x => (x % 2 == 0 && keepEvenNumbers) || (x % 2 == 1 && keepOddNumbers))
    val transformedRDD = filteredRDD.map(x => Row(x))

    // create a schema with a single non-nullable integer column using the configured column name
    val schema = StructType(Array(StructField(columnName, IntegerType, true)))

    sqlContext.createDataFrame(transformedRDD, schema)
  }
}
