package com.memsql.spark.etl.api

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming._
import com.memsql.spark.etl.api.configs.{TestJsonExtractConfig, TestStringExtractConfig, PhaseConfig}
import com.memsql.spark.etl.utils.ByteUtils._

/*
 * A simple Extractor for testing.  Produces the same fixed RDD every interval.
 */
class ConstantExtractor(rdd: RDD[Array[Byte]]) extends ByteArrayExtractor {
  override def extract(ssc: StreamingContext, config: PhaseConfig, batchInterval: Long, logger: Logger): InputDStream[Array[Byte]] = new ConstantInputDStream(ssc, rdd)
}

class ConfigStringExtractor extends ByteArrayExtractor {
  override def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long, logger: Logger): InputDStream[Array[Byte]] = {
    val str = extractConfig match {
      case strConfig: TestStringExtractConfig => extractConfig.asInstanceOf[TestStringExtractConfig].value
      case jsonConfig: TestJsonExtractConfig => extractConfig.asInstanceOf[TestJsonExtractConfig].value.toString
    }

    val rdd = ssc.sparkContext.parallelize(Seq(utf8StringToBytes(str)))
    new ConstantInputDStream(ssc, rdd)
  }
}
