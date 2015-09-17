package com.memsql.spark.phases

import com.memsql.spark.etl.api.{ByteArrayExtractor, PhaseConfig}
import com.memsql.spark.etl.utils.ByteUtils._
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

case class TestLinesExtractConfig(value: String) extends PhaseConfig

/*
 * A simple Extractor for testing.  Produces the same fixed RDD every interval.
 * The RDD is the result of splitting the user config by lines.
 */
class TestLinesExtractor extends ByteArrayExtractor {
  override def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long, logger: PhaseLogger): InputDStream[Array[Byte]] = {
    val lines = extractConfig.asInstanceOf[TestLinesExtractConfig].value.split("\\r?\\n")
    val rdd = ssc.sparkContext.parallelize(lines.map(utf8StringToBytes(_)))
    new ConstantInputDStream(ssc, rdd)
  }
}
