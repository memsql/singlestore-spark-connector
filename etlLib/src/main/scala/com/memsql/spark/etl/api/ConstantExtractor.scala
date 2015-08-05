package com.memsql.spark.etl.api

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream._
import scala.reflect.ClassTag
import org.apache.spark.streaming._
import com.memsql.spark.etl.api.configs.PhaseConfig

class ConstantStream[T: ClassTag](ssc: StreamingContext, rdd: RDD[T]) extends InputDStream[T](ssc) {
  override def compute(validTime: Time): Option[RDD[T]] = Some(rdd)

  override def start() = {}
  override def stop()  = {}
}

/*
 * A simple Extractor for testing.  Produces the same fixed RDD every interval.
 */
class ConstantExtractor[T: ClassTag](rdd: RDD[T]) extends Extractor[T] { 
  override def extract(ssc: StreamingContext, config: PhaseConfig, batchInterval: Long): InputDStream[T] = new ConstantStream(ssc, rdd)
}
