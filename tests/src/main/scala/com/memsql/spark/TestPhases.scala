package com.memsql.spark

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.sql._
import com.memsql.spark.etl.api._
import com.memsql.spark.etl.utils.PhaseLogger

/**
 * Fake Extractor/Transformer classes for testing JarInspector. See `dockertest/jar_inspector_test.py`
 */

// Classes which extend our abstract classes should be detected
class Extractor1 extends SimpleByteArrayExtractor {
  override def nextRDD(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Option[RDD[Array[Byte]]] = None
}

class Extractor2 extends ByteArrayExtractor {
  override def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long, logger: PhaseLogger): InputDStream[Array[Byte]] = null
}

class Extractor3 extends Extractor {
  override def next(ssc: StreamingContext, time: Long, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
                    logger: PhaseLogger): Option[DataFrame] = None
}

// Classes which extend a user's abstract classes should also be detected
abstract class AbstractExtractor4 extends SimpleByteArrayExtractor
class Extractor4 extends AbstractExtractor4 {
  override def nextRDD(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Option[RDD[Array[Byte]]] = None
}

abstract class AbstractExtractor5 extends ByteArrayExtractor
class Extractor5 extends AbstractExtractor5 {
  override def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long, logger: PhaseLogger): InputDStream[Array[Byte]] = null
}

abstract class AbstractExtractor6 extends Extractor
class Extractor6 extends AbstractExtractor6 {
  override def next(ssc: StreamingContext, time: Long, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
                    logger: PhaseLogger): Option[DataFrame] = None
}

// Nested abstract classes should also be detected
abstract class NestedAbstractExtractor extends AbstractExtractor4
class Extractor7 extends AbstractExtractor4 {
  override def nextRDD(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Option[RDD[Array[Byte]]] = None
}


// Same goes for Transformers
class Transformer1 extends SimpleByteArrayTransformer {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: UserTransformConfig, logger: PhaseLogger): DataFrame = null
}

class Transformer2 extends ByteArrayTransformer {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: PhaseConfig, logger: PhaseLogger): DataFrame = null
}

class Transformer3 extends Transformer {
  override def transform(sqlContext: SQLContext, df: DataFrame, config: PhaseConfig, logger: PhaseLogger): DataFrame = null
}

abstract class AbstractTransformer4 extends SimpleByteArrayTransformer
class Transformer4 extends AbstractTransformer4 {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: UserTransformConfig, logger: PhaseLogger): DataFrame = null
}

abstract class AbstractTransformer5 extends ByteArrayTransformer
class Transformer5 extends AbstractTransformer5 {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: PhaseConfig, logger: PhaseLogger): DataFrame = null
}

abstract class AbstractTransformer6 extends Transformer
class Transformer6 extends AbstractTransformer6 {
  override def transform(sqlContext: SQLContext, df: DataFrame, config: PhaseConfig, logger: PhaseLogger): DataFrame = null
}

abstract class NestedAbstractTransformer extends AbstractTransformer4
class Transformer7 extends NestedAbstractTransformer {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: UserTransformConfig, logger: PhaseLogger): DataFrame = null
}
