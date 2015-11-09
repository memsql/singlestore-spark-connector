package com.memsql.spark.interface.support

import com.memsql.spark.etl.api._
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

class DummyExtractor extends SimpleByteArrayExtractor {
  override def nextRDD(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Option[RDD[Array[Byte]]] = None
}


class DummyTransformer extends SimpleByteArrayTransformer {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], transformConfig: UserTransformConfig,
                         logger: PhaseLogger): DataFrame = null
}
