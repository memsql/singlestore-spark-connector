package com.memsql.spark.interface.supportClasses

import com.memsql.spark.etl.api._
import com.memsql.spark.etl.utils.{SimpleJsonSchema, PhaseLogger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by marin on 10/1/15.
 */
class DummyExtractor extends SimpleByteArrayExtractor {
  override def nextRDD(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Option[RDD[Array[Byte]]] = None
}


class DummyTransformer extends SimpleByteArrayTransformer {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], transformConfig: UserTransformConfig,
                         logger: PhaseLogger): DataFrame = null
}
