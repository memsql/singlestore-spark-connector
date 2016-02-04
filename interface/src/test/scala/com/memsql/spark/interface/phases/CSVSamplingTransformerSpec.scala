// scalastyle:off magic.number
package com.memsql.spark.interface.phases

import com.memsql.spark.connector.dataframe._
import com.memsql.spark.etl.LocalSparkContext
import com.memsql.spark.etl.utils.ByteUtils._
import com.memsql.spark.interface.TestKitSpec
import com.memsql.spark.interface.util.PipelineLogger
import com.memsql.spark.phases.{CSVSamplingTransformerConfig, CSVSamplingTransformer, CSVTransformerException}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Duration, StreamingContext}
import spray.json._

class CSVSamplingTransformerSpec extends TestKitSpec("CSVSamplingTransformerSpec") with LocalSparkContext{
  val DURATION_CONST = 5000
  val transformer = new CSVSamplingTransformer
  var sqlContext: SQLContext = null
  var streamingContext: StreamingContext = null
  var logger: PipelineLogger = null

  override def beforeEach(): Unit = {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Test")
    sc = new SparkContext(sparkConfig)
    sqlContext = new SQLContext(sc)
    streamingContext = new StreamingContext(sc, new Duration(DURATION_CONST))
    logger = new PipelineLogger(s"Pipeline p1 transform", false)
  }

  "CSVSamplingTransformer" should {
    "work on DataFrame with String column" in {
      val config = CSVSamplingTransformerConfig(
        delimiter = Some(','),
        escape = Some("\\"),
        quote = Some('\''),
        null_string = Some("NULL"),
        sample_size = Some(10),
        has_headers = Some(false)
      )

      val schema = StructType(StructField("string", StringType, false) :: Nil)
      val sampleData = List(
        "1,hello",
        "2,world",
        "3,foo",
        "4,bar"
      )
      val rowRDD = sqlContext.sparkContext.parallelize(sampleData).map(Row(_))
      val dfIn = sqlContext.createDataFrame(rowRDD, schema)

      val df = transformer.transform(sqlContext, dfIn, config, logger)
      val schemaOut = StructType(
        StructField("column_1", StringType, true) ::
          StructField("column_2", StringType, true) :: Nil)

      assert(df.schema == schemaOut)
      assert(df.first == Row("1", "hello"))
      assert(df.count == 4)
    }
  }

  "only return a sample of large DataFrames" in {
    val config = CSVSamplingTransformerConfig(
      delimiter = Some(','),
      escape = Some("\\"),
      quote = Some('\''),
      null_string = Some("NULL"),
      sample_size = Some(10),
      has_headers = Some(false)
    )

    val schema = StructType(StructField("string", StringType, false) :: Nil)
    val sampleData = (0 to 100).map(x => s"$x,test")
    val rowRDD = sqlContext.sparkContext.parallelize(sampleData).map(Row(_))
    val dfIn = sqlContext.createDataFrame(rowRDD, schema)

    val df = transformer.transform(sqlContext, dfIn, config, logger)
    val schemaOut = StructType(
      StructField("column_1", StringType, true) ::
        StructField("column_2", StringType, true) :: Nil)

    assert(df.schema == schemaOut)
    assert(df.first == Row("0", "test"))
    assert(df.count == 10)
  }

  "skip header lines when so instructed" in {
    val config = CSVSamplingTransformerConfig(
      delimiter = Some(','),
      escape = Some("\\"),
      quote = Some('\''),
      null_string = Some("NULL"),
      sample_size = Some(10),
      has_headers = Some(true)
    )

    val schema = StructType(StructField("string", StringType, false) :: Nil)
    val sampleData = List(
      "id,data",
      "1,hello",
      "2,world",
      "3,foo",
      "4,bar"
    )
    val rowRDD = sqlContext.sparkContext.parallelize(sampleData).map(Row(_))
    val dfIn = sqlContext.createDataFrame(rowRDD, schema)

    val df = transformer.transform(sqlContext, dfIn, config, logger)
    val schemaOut = StructType(
      StructField("id", StringType, true) ::
        StructField("data", StringType, true) :: Nil)

    assert(df.schema == schemaOut)
    assert(df.first == Row("1", "hello"))
    assert(df.count == 4)
  }

  "fail on variable-sized rows" in {
    val config = CSVSamplingTransformerConfig(
      delimiter = Some(','),
      escape = Some("\\"),
      quote = Some('\''),
      null_string = Some("NULL"),
      sample_size = Some(10),
      has_headers = Some(false)
    )

    val schema = StructType(StructField("string", StringType, false) :: Nil)
    val sampleData = List(
      "1,hello",
      "2,foo,bar"
    )

    val rowRDD = sqlContext.sparkContext.parallelize(sampleData).map(Row(_))
    val dfIn = sqlContext.createDataFrame(rowRDD, schema)
    try {
      transformer.transform(sqlContext, dfIn, config, logger)
    } catch {
      case e: Throwable => assert(e.isInstanceOf[CSVTransformerException])
    }
  }
}
