package com.memsql.spark.interface

import java.io.File
import java.sql.Timestamp

import akka.pattern.ask
import akka.actor.Props
import akka.util.Timeout
import com.memsql.spark.connector.dataframe.{JsonType, JsonValue}
import com.memsql.spark.etl.api.{UserTransformConfig, PhaseConfig, ByteArrayTransformer, UserExtractConfig}
import com.memsql.spark.etl.api.configs._
import com.memsql.spark.etl.utils.ByteUtils._
import ExtractPhaseKind._
import TransformPhaseKind._
import LoadPhaseKind._
import com.memsql.spark.etl.utils.PhaseLogger
import com.memsql.spark.interface.api._
import ApiActor._
import com.memsql.spark.interface.util.{PipelineLogger, Paths}
import com.memsql.spark.phases.{TestLinesExtractConfig, ZookeeperManagedKafkaExtractConfig, JsonTransformConfig}
import com.memsql.spark.phases.configs.ExtractPhase
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Duration, StreamingContext}
import spray.json._
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import com.memsql.spark.etl.utils.Logging

class DuplicateTransformer extends ByteArrayTransformer {
  var columnName: String = "testcol"
  val DUPLICATION_FACTOR = 100

  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], transformConfig: PhaseConfig, logger: PhaseLogger): DataFrame = {
    val transformedRDD = rdd.flatMap(r => List.fill(DUPLICATION_FACTOR)(Row(new JsonValue(byteUtils.bytesToUTF8String(r)))))
    val schema = StructType(Array(StructField(columnName, JsonType, true)))
    sqlContext.createDataFrame(transformedRDD, schema)
  }
}

// scalastyle:off magic.number
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
      ExtractPhaseKind.ZookeeperManagedKafka,
      ExtractPhase.writeConfig(
        ExtractPhaseKind.ZookeeperManagedKafka, ZookeeperManagedKafkaExtractConfig(List("test:2181"), "topic"))),
    Phase[TransformPhaseKind](
      TransformPhaseKind.Json,
      TransformPhase.writeConfig(
        TransformPhaseKind.Json, JsonTransformConfig("data"))),
    Phase[LoadPhaseKind](
      LoadPhaseKind.MemSQL,
      LoadPhase.writeConfig(
        LoadPhaseKind.MemSQL, MemSQLLoadConfig("db", "table", None, None))))

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
          val rows = Array(
            Row(1, "test 1", Timestamp.valueOf("1973-07-09 17:11:51.111"), true),
            Row(2, "test 2", Timestamp.valueOf("1977-01-15 16:23:42.222"), true),
            Row(3, null,     Timestamp.valueOf("1980-07-24 17:35:33.333"), false)
          )
          val df = sqlContext.createDataFrame(sc.parallelize(rows), schema)

          val (columns, records) = pm.getTransformRecords(df, rows.size)
          assert(columns.get == List(("val_int", "integer"), ("val_string", "string"), ("val_datetime", "timestamp"), ("val_bool", "boolean")))
          // We should take the first 10 records from the above RDD.
          val record1 = records.get(0)
          assert(record1(0) == "1")
          assert(record1(1) == "test 1")
          assert(record1(2) == "1973-07-09 17:11:51.111")
          assert(record1(3) == "true")
          val record2 = records.get(1)
          assert(record2(0) == "2")
          assert(record2(1) == "test 2")
          assert(record2(2) == "1977-01-15 16:23:42.222")
          assert(record2(3) == "true")
          val record3 = records.get(2)
          assert(record3(0) == "3")
          assert(record3(1) == "null")
          assert(record3(2) == "1980-07-24 17:35:33.333")
          assert(record3(3) == "false")
        }
        case Failure(error) => fail(s"Expected pipeline pipeline2 to exist: $error")
      }
    }
  }

  "Transform truncation test" should {
    "truncate the transformed result" in {
      val transformTestConfig = PipelineConfig(
        Phase[ExtractPhaseKind](
          ExtractPhaseKind.TestLines,
          ExtractPhase.writeConfig(
            ExtractPhaseKind.TestLines, TestLinesExtractConfig("testtest\ntest\ntest\ntest\ntest\ntest"))),
        Phase[TransformPhaseKind](
          TransformPhaseKind.User,
          TransformPhase.writeConfig(
            TransformPhaseKind.User, UserTransformConfig(class_name = "com.memsql.spark.interface.DuplicateTransformer", value = JsString("test")))),
        Phase[LoadPhaseKind](
          LoadPhaseKind.MemSQL,
          LoadPhase.writeConfig(
            LoadPhaseKind.MemSQL, MemSQLLoadConfig("db", "table", None, None))))

      apiRef ? PipelinePut("pipeline3", batch_interval = 1, config = transformTestConfig)

      apiRef ! PipelineUpdate("pipeline3", trace_batch_count = Some(5))
      receiveOne(1.second).asInstanceOf[Try[Boolean]] match {
        case Success(resp) => assert(resp)
        case Failure(err) => fail(s"unexpected response $err")
      }

      whenReady((apiRef ? PipelineGet("pipeline3")).mapTo[Try[Pipeline]]) {
        case Success(pipeline) => {
          val pm = new DefaultPipelineMonitor(apiRef, pipeline, sc, streamingContext)
          pm.ensureStarted
          assert(pm.isAlive())

          Thread.sleep(2000)
          val q = pipeline.metricsQueue

          while (!q.isEmpty) {
            val event = q.dequeue
            event.event_type match {
              case PipelineEventType.BatchEnd => {
                event.asInstanceOf[BatchEndEvent].transform match {
                  case None => fail("BatchEndEvent does not contain a transform record")
                  case Some(x) => {
                    x.records match {
                      case None => fail("transform does not contain records")
                      case Some(y) => assert(y.size < 20)
                    }
                  }
                }
              }
              case _ =>
            }
          }

          pm.stop
        }
        case Failure(error) => fail(s"Expected pipeline pipeline3 to exist: $error")
      }
    }
  }
}
