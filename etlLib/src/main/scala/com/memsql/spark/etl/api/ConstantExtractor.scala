package com.memsql.spark.etl.api

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream._
import scala.reflect.ClassTag
import org.apache.spark.streaming._
import com.memsql.spark.etl.api.configs.{TestJsonExtractConfig, TestStringExtractConfig, PhaseConfig}
import com.memsql.spark.etl.utils.ByteUtils._

class ConstantStream[T: ClassTag](ssc: StreamingContext, rdd: RDD[T]) extends InputDStream[T](ssc) {
  override def compute(validTime: Time): Option[RDD[T]] = Some(rdd)

  override def start() = {}
  override def stop()  = {}
}

/*
 * A simple Extractor for testing.  Produces the same fixed RDD every interval.
 */
class ConstantExtractor(rdd: RDD[Array[Byte]]) extends ByteArrayExtractor {
  override def extract(ssc: StreamingContext, config: PhaseConfig, batchInterval: Long): InputDStream[Array[Byte]] = new ConstantStream(ssc, rdd)
}

class ConfigStringExtractor extends ByteArrayExtractor {
  override def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long): InputDStream[Array[Byte]] = {
    val str = extractConfig match {
      case strConfig: TestStringExtractConfig => extractConfig.asInstanceOf[TestStringExtractConfig].value
      case jsonConfig: TestJsonExtractConfig => extractConfig.asInstanceOf[TestJsonExtractConfig].value.toString
    }

    val rdd = ssc.sparkContext.parallelize(Seq(utf8StringToBytes(str)))
    new ConstantStream(ssc, rdd)
  }
}
