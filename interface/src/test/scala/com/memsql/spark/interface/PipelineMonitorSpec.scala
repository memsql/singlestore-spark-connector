package com.memsql.spark.interface

import java.text.SimpleDateFormat
import java.io.File

import akka.pattern.ask
import akka.actor.Props
import akka.util.Timeout
import com.memsql.spark.etl.api.configs._
import com.memsql.spark.etl.utils.ByteUtils._
import ExtractPhaseKind._
import TransformPhaseKind._
import LoadPhaseKind._
import com.memsql.spark.interface.api.{Pipeline, PipelineState, ApiActor}
import ApiActor._
import com.memsql.spark.interface.util.{Paths, JarLoaderException}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Duration, StreamingContext}
import ooyala.common.akka.web.JsonUtils._
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import spray.json._

class PipelineMonitorSpec extends TestKitSpec("PipelineMonitorSpec") with LocalSparkContext {
  val apiRef = system.actorOf(Props[ApiActor], "api")
  var sqlContext: SQLContext = _
  var streamingContext: StreamingContext = _
  implicit val timeout = Timeout(5.seconds)

  override def beforeEach(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
    streamingContext = new StreamingContext(sc, new Duration(5000))
  }

  val localJarFile = Paths.join(new File(".").getCanonicalPath, s"target/scala-2.10/MemSQL-assembly-${Main.VERSION}.jar")

  val config = PipelineConfig(
    Phase[ExtractPhaseKind](
      ExtractPhaseKind.Kafka,
      ExtractPhase.writeConfig(
        ExtractPhaseKind.Kafka, KafkaExtractConfig("test", 9092, "topic"))),
    Phase[TransformPhaseKind](
      TransformPhaseKind.Json,
      TransformPhase.writeConfig(
        TransformPhaseKind.Json, JsonTransformConfig("data"))),
    Phase[LoadPhaseKind](
      LoadPhaseKind.MemSQL,
      LoadPhase.writeConfig(
        LoadPhaseKind.MemSQL, MemSQLLoadConfig("db", "table", None, None, None, None))))

  "PipelineMonitor" should {
    "create a monitor if the class can be properly loaded" in {
      apiRef ? PipelinePut("pipeline2", batch_interval=10, config=config)
      whenReady((apiRef ? PipelineGet("pipeline2")).mapTo[Try[Pipeline]]) {
        case Success(pipeline) => {
          val pm = new DefaultPipelineMonitor(apiRef, pipeline, sc, streamingContext)
          assert(pm.pipeline_id == "pipeline2")
          assert(!pm.isAlive)
          assert(pipeline.state == PipelineState.RUNNING)
        }
        case Failure(error) => fail(s"Expected pipeline pipeline2 to exist: $error")
      }
    }

    "fail to create a monitor if the class cannot be loaded" in {
      val config2 = config.copy(extract = Phase[ExtractPhaseKind](
        ExtractPhaseKind.User,
        ExtractPhase.writeConfig(
          ExtractPhaseKind.User, UserExtractConfig("com.test.Extract", ""))),
        jar = Some("/doesnt_exist/test.jar"))

      //create pipeline which requires loading a class from the jar and try to load in a PipelineMonitor
      apiRef ! PipelinePut("pipeline1", batch_interval=100, config=config2)
      whenReady((apiRef ? PipelineGet("pipeline1")).mapTo[Try[Pipeline]]) {
        case Success(pipeline) => {
          intercept[JarLoaderException] {
            new DefaultPipelineMonitor(apiRef, pipeline, sc, streamingContext)
          }
        }
        case Failure(error) => fail(s"Expected pipeline pipeline1 to exist: $error")
      }
    }
  }

  "getExtractRecords" should {
    "be able to create a list of records given an RDD" in {
      apiRef ? PipelinePut("pipeline2", batch_interval=10, config=config)
      whenReady((apiRef ? PipelineGet("pipeline2")).mapTo[Try[Pipeline]]) {
        case Success(pipeline) => {
          val pm = new DefaultPipelineMonitor(apiRef, pipeline, sc, streamingContext)
          val rdd1 = sc.parallelize(0 until 10).map(x => utf8StringToBytes(x.toString))
          val (columns1, records1) = pm.getExtractRecords(rdd1)
          assert(columns1.get == List(("value", "string")))
          assert(records1.get.map(record => record(0).toInt) == (0 until 10))

          val rdd2 = sc.parallelize(Array(utf8StringToBytes("test 1")))
          val (columns2, records2) = pm.getExtractRecords(rdd2)
          assert(columns1.get == List(("value", "string")))
          assert(records2.get == List(List("test 1")))

          val rdd3 = sc.parallelize(Array(Array(127.toByte, 127.toByte)))
          val (columns3, records3) = pm.getExtractRecords(rdd3)
          assert(columns1.get == List(("value", "string")))
          assert(records3.get == List(List("\\x7f\\x7f")))
        }
        case Failure(error) => fail(s"Expected pipeline pipeline2 to exist: $error")
      }
    }
  }

  "getTransformRecords" should {
    "be able to create a list of records given a DataFrame" in {
      apiRef ? PipelinePut("pipeline2", batch_interval=10, config=config)
      whenReady((apiRef ? PipelineGet("pipeline2")).mapTo[Try[Pipeline]]) {
        case Success(pipeline) => {
          val pm = new DefaultPipelineMonitor(apiRef, pipeline, sc, streamingContext)

          val schema = StructType(Array(
            StructField("val_int", IntegerType, false),
            StructField("val_string", StringType, true),
            StructField("val_datetime", TimestampType, false),
            StructField("val_bool", BooleanType, false)
          ))
          val dateFormat = new SimpleDateFormat()
          val rows = Array(
            Row(1, "test 1", dateFormat.format(111111111111L), true),
            Row(2, "test 2", dateFormat.format(222222222222L), true),
            Row(3, null, dateFormat.format(333333333333L), false)
          )
          val df = sqlContext.createDataFrame(sc.parallelize(rows), schema)

          val (columns, records) = pm.getTransformRecords(df)
          assert(columns.get == List(("val_int", "integer"), ("val_string", "string"), ("val_datetime", "timestamp"), ("val_bool", "boolean")))
          // We should take the first 10 records from the above RDD.
          val record1 = records.get(0)
          assert(record1(0) == "1")
          assert(record1(1) == "test 1")
          assert(record1(2) == dateFormat.format(111111111111L).toString)
          assert(record1(3) == "true")
          val record2 = records.get(1)
          assert(record2(0) == "2")
          assert(record2(1) == "test 2")
          assert(record2(2) == dateFormat.format(222222222222L).toString)
          assert(record2(3) == "true")
          val record3 = records.get(2)
          assert(record3(0) == "3")
          assert(record3(1) == "null")
          assert(record3(2) == dateFormat.format(333333333333L).toString)
          assert(record3(3) == "false")
        }
        case Failure(error) => fail(s"Expected pipeline pipeline2 to exist: $error")
      }
    }
  }
}
