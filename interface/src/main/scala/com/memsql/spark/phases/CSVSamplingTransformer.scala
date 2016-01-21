package com.memsql.spark.phases

import com.memsql.spark.etl.api.PhaseConfig
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.commons.csv._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.util._

import scala.collection.JavaConversions._

case class CSVSamplingTransformerConfig(
  delimiter: Option[Char],
  escape: Option[String],
  quote: Option[Char],
  null_string: Option[String],
  sample_size: Option[Int]) extends PhaseConfig

class CSVSamplingTransformer extends CSVTransformerBase {
  val DEFAULT_SAMPLE_SIZE = 10

  override def transform(sqlContext: SQLContext, rdd: RDD[String], transformConfig: PhaseConfig, logger: PhaseLogger): DataFrame = {
    val config = transformConfig.asInstanceOf[CSVSamplingTransformerConfig]
    val csvFormat = getCSVFormat(config.delimiter, config.escape, config.quote)

    val sampledRDD = sqlContext.sparkContext.parallelize(rdd.take(config.sample_size.getOrElse(DEFAULT_SAMPLE_SIZE)))
    val nulledRDD = getNulledRDD(sampledRDD, csvFormat, config.null_string)

    if (nulledRDD.isEmpty) {
        throw new CSVTransformerException("Input RDD is empty")
    }

    val firstRow = nulledRDD.first()
    val schema = StructType(firstRow.zipWithIndex.map{ case(x, i) => StructField(s"column_${i + 1}", StringType, true) })

    val rowRDD = nulledRDD.zipWithIndex.map{ case(x, i) => {
      if (x.size != firstRow.size) {
        throw new CSVTransformerException(s"Row ${i + 1} has length ${x.size} but the first row has length ${firstRow.size}")
      }
      Row.fromSeq(x)
    }}
    sqlContext.createDataFrame(rowRDD, schema)
  }
}
