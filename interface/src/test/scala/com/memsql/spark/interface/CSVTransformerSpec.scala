package com.memsql.spark.interface

import com.memsql.spark.etl.api.PhaseConfig
import com.memsql.spark.etl.utils.ByteUtils._
import com.memsql.spark.etl.utils.PhaseLogger
import com.memsql.spark.interface.util.PipelineLogger
import com.memsql.spark.phases.{CSVTransformerConfig, CSVTransformer, CSVTransformerException}
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import spray.json._
import scala.util.{Success, Failure}
import scala.concurrent.duration._

class CSVTransformerSpec extends TestKitSpec("CSVTransformerSpec") with LocalSparkContext{
  val DURATION_CONST = 5000
  val transformer = new CSVTransformer

  override def beforeEach(): Unit = {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Test")
    sc = new SparkContext(sparkConfig)
  }

  "CSVTransformer" should {
    "allow the empty character for escape" in {
      val config = CSVTransformerConfig(
        delimiter = Some('/'),
        escape = Some(""),
        quote = Some('\''),
        null_string = Some("NULL"),
        columns = """[{"name": "sample_column", "column_type": "string"}]""".parseJson
      )

      val sqlContext = new SQLContext(sc)
      val streamingContext = new StreamingContext(sc, new Duration(DURATION_CONST))
      val rdd = sc.parallelize(List("test\\default")).map(x => utf8StringToBytes(x))
      val logger = new PipelineLogger(s"Pipeline p1 transform", false)

      try {
        val df = transformer.transform(sqlContext, rdd, config, logger)
        assert(df.rdd.collect()(0)(0).toString == "test\\default", "Input not preserved");
      } catch {
        case e: CSVTransformerException => fail("CSVTransformer exception")
        case e: Throwable => fail(s"Unexpected response $e")
      }
    }

    "fail on an invalid column json" in {
      val config = CSVTransformerConfig(
        delimiter = Some('/'),
        escape = Some("\\"),
        quote = Some('\''),
        null_string = Some("NULL"),
        columns = """{"name": "sample_column", "column_type": "string"}""".parseJson
      )

      val sqlContext = new SQLContext(sc)
      val streamingContext = new StreamingContext(sc, new Duration(DURATION_CONST))
      val rdd = sc.parallelize(List("test\\default")).map(x => utf8StringToBytes(x))
      val logger = new PipelineLogger(s"Pipeline p1 transform", false)

      try {
        transformer.transform(sqlContext, rdd, config, logger)
      } catch {
        case e: Throwable => assert(e.isInstanceOf[DeserializationException])
      }
    }

    "fail if the escape is longer than 1 character" in {
      val config = CSVTransformerConfig(
        delimiter = Some('/'),
        escape = Some("\\\\"),
        quote = Some('\''),
        null_string = Some("NULL"),
        columns = """[{"name": "sample_column", "column_type": "string"}]""".parseJson
      )

      val sqlContext = new SQLContext(sc)
      val streamingContext = new StreamingContext(sc, new Duration(DURATION_CONST))
      val rdd = sc.parallelize(List("test\\default")).map(x => utf8StringToBytes(x))
      val logger = new PipelineLogger(s"Pipeline p1 transform", false)

      try {
        transformer.transform(sqlContext, rdd, config, logger)
      } catch {
        case e: Throwable => assert(e.isInstanceOf[CSVTransformerException])
      }
    }
  }
}
