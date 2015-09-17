package com.memsql.spark.etl.api

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.rdd.RDD
import com.memsql.spark.etl.utils.{ByteUtils, PhaseLogger}

abstract class Transformer[S] extends Serializable {
  def initialize(sqlContext: SQLContext, transformConfig: PhaseConfig, logger: PhaseLogger): Unit = {}
  def transform(sqlContext: SQLContext, rdd: RDD[S], transformConfig: PhaseConfig, logger: PhaseLogger): DataFrame
}

abstract class ByteArrayTransformer extends Transformer[Array[Byte]] {
  final var byteUtils = ByteUtils
}

abstract class SimpleByteArrayTransformer extends ByteArrayTransformer {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: PhaseConfig, logger: PhaseLogger): DataFrame = {
    val userConfig = config.asInstanceOf[UserTransformConfig]
    transform(sqlContext, rdd, userConfig, logger)
  }

  def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: UserTransformConfig, logger: PhaseLogger): DataFrame
}
