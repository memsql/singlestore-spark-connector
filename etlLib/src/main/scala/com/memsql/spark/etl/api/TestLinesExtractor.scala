package com.memsql.spark.etl.api

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming._
import com.memsql.spark.etl.api.configs.{TestLinesExtractConfig, PhaseConfig}
import com.memsql.spark.etl.utils.ByteUtils._

/*
 * A simple Extractor for testing.  Produces the same fixed RDD every interval.
 * The RDD is the result of splitting the user config by lines.
 */
class TestLinesExtractor extends ByteArrayExtractor {
  override def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long, logger: Logger): InputDStream[Array[Byte]] = {
    val lines = extractConfig.asInstanceOf[TestLinesExtractConfig].value.split("\\r?\\n")
    val rdd = ssc.sparkContext.parallelize(lines.map(utf8StringToBytes(_)))
    new ConstantInputDStream(ssc, rdd)
  }
}
