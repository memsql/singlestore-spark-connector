package com.memsql.spark.phases

import com.memsql.spark.etl.api.{Extractor, PhaseConfig}
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

case class TestLinesExtractConfig(value: String) extends PhaseConfig

/*
 * A simple Extractor for testing.  Produces the same fixed DataFrame every interval.
 * The DataFrame is the result of splitting the user config by lines.
 */
class TestLinesExtractor extends Extractor {
  override def next(ssc: StreamingContext, time: Long, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
           logger: PhaseLogger): Option[DataFrame] = {
    val schema = StructType(StructField("line", StringType, false) :: Nil)

    val lines = config.asInstanceOf[TestLinesExtractConfig].value.split("\\r?\\n")
    val rowRDD = sqlContext.sparkContext.parallelize(lines).map(Row(_))

    val df = sqlContext.createDataFrame(rowRDD, schema)
    Some(df)
  }
}
