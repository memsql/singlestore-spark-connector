// scalastyle:off magic.number
package com.memsql.spark.interface

import java.io.File
import java.sql.Timestamp

import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout
import com.memsql.spark.connector.dataframe.{JsonType, JsonValue}
import com.memsql.spark.etl.api.configs.ExtractPhaseKind._
import com.memsql.spark.etl.api.configs.LoadPhaseKind._
import com.memsql.spark.etl.api.configs.TransformPhaseKind._
import com.memsql.spark.etl.api.configs._
import com.memsql.spark.etl.api.{PhaseConfig, Transformer, UserTransformConfig}
import com.memsql.spark.etl.utils.PhaseLogger
import com.memsql.spark.interface.api.ApiActor._
import com.memsql.spark.interface.api._
import com.memsql.spark.interface.util.Paths
import com.memsql.spark.phases.configs.ExtractPhase
import com.memsql.spark.phases._
import org.apache.spark.sql.memsql.test.TestBase
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.scalatest.BeforeAndAfterAll
import spray.json._

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class DuplicateTransformer extends Transformer {
  var columnName: String = "testcol"
  val DUPLICATION_FACTOR = 100

  override def transform(sqlContext: SQLContext, df: DataFrame, config: PhaseConfig, logger: PhaseLogger): DataFrame = {
    val transformedRDD = df.rdd.flatMap(r => List.fill(DUPLICATION_FACTOR)(Row(new JsonValue(r.toSeq.head.asInstanceOf[String]))))
    val schema = StructType(Array(StructField(columnName, JsonType, true)))
    sqlContext.createDataFrame(transformedRDD, schema)
  }
}

class SlowTransformer extends Transformer {
  override def transform(sqlContext: SQLContext, df: DataFrame, config: PhaseConfig, logger: PhaseLogger): DataFrame = {
    Thread.sleep(10000L)
    df
  }
}

class PipelineMonitorSpec extends TestKitSpec("PipelineMonitorSpec") with TestBase with BeforeAndAfterAll {
  val apiRef = system.actorOf(Props(classOf[ApiActor], new SparkProgress()), "api")
  var streamingContext: StreamingContext = _
  implicit val timeout = Timeout(5.seconds)

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkUp(true)
    streamingContext = new StreamingContext(sc, new Duration(5000))
  }

  override def afterAll(): Unit = {
    sparkDown
    super.afterAll()
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
        LoadPhaseKind.MemSQL, MemSQLLoadConfig(dbName, "t2", None, None))))

  "PipelineMonitor" should {
    "create a monitor if the class can be properly loaded" in {
      apiRef ? PipelinePut("pipeline2", single_step=None, batch_interval=Some(10), config=config)
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
      apiRef ? PipelinePut("pipeline2", single_step=None, batch_interval=Some(10), config=config)
      whenReady((apiRef ? PipelineGet("pipeline2")).mapTo[Try[Pipeline]]) {
        case Success(pipeline) => {
          val pm = new DefaultPipelineMonitor(apiRef, pipeline, sc, streamingContext)
          val rdd1 = sc.parallelize(0 until 10).map(x => Row(x.toString))
          val schema1 = StructType(StructField("value", StringType, false) :: Nil)
          val df1 = sqlContext.createDataFrame(rdd1, schema1)
          val (columns1, records1) = pm.getExtractRecords(df1)
          assert(columns1.get == List(("value", "string")))
          assert(records1.get.map(record => record(0).toInt) == (0 until 10))

          val rdd2 = sc.parallelize(Array(Row("test 1", 1, false, null)))
          val schema2 = StructType(Seq(StructField("col1", StringType, false),
                                       StructField("col2", IntegerType, false),
                                       StructField("col3", BooleanType, false),
                                       StructField("col4", StringType, true)))
          val df2 = sqlContext.createDataFrame(rdd2, schema2)
          val (columns2, records2) = pm.getExtractRecords(df2)
          assert(columns2.get == List(("col1", "string"), ("col2", "integer"), ("col3", "boolean"), ("col4", "string")))
          assert(records2.get == List(List("test 1", "1", "false", "null")))

          val rdd3 = sc.parallelize(Array(Row(Array(127.toByte, 127.toByte), null)))
          val schema3 = StructType(Seq(StructField("bytes1", BinaryType, false), StructField("bytes2", BinaryType, true)))
          val df3 = sqlContext.createDataFrame(rdd3, schema3)
          val (columns3, records3) = pm.getExtractRecords(df3)
          assert(columns3.get == List(("bytes1", "binary"), ("bytes2", "binary")))
          assert(records3.get == List(List("\\x7f\\x7f", "null")))
        }
        case Failure(error) => fail(s"Expected pipeline pipeline2 to exist: $error")
      }
    }
  }

  "getTransformRecords" should {
    "be able to create a list of records given a DataFrame" in {
      apiRef ? PipelinePut("pipeline2", single_step=None, batch_interval=Some(10), config=config)
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
            ExtractPhaseKind.TestLines, TestLinesExtractConfig("\"testtest\"\n\"test\"\n\"test\"\n\"test\"\n\"test\"\n\"test\""))),
        Phase[TransformPhaseKind](
          TransformPhaseKind.User,
          TransformPhase.writeConfig(
            TransformPhaseKind.User, UserTransformConfig(class_name = "com.memsql.spark.interface.DuplicateTransformer", value = JsString("test")))),
        Phase[LoadPhaseKind](
          LoadPhaseKind.MemSQL,
          LoadPhase.writeConfig(
            LoadPhaseKind.MemSQL, MemSQLLoadConfig(dbName, "t3", None, None))))

      apiRef ? PipelinePut("pipeline3", single_step = None, batch_interval = Some(1), config = transformTestConfig)

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

          var foundEvent = false
          while (!q.isEmpty) {
            val event = q.dequeue
            event.event_type match {
              case PipelineEventType.BatchEnd => {
                foundEvent = true
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

          assert(foundEvent)

          pm.stop
        }
        case Failure(error) => fail(s"Expected pipeline pipeline3 to exist: $error")
      }
    }
  }

  "Batch cancellation test" should {
    "emit a cancellation event" in {
      val transformTestConfig = PipelineConfig(
        Phase[ExtractPhaseKind](
          ExtractPhaseKind.TestLines,
          ExtractPhase.writeConfig(
            ExtractPhaseKind.TestLines, TestLinesExtractConfig("testtest\ntest\ntest\ntest\ntest\ntest"))),
        Phase[TransformPhaseKind](
          TransformPhaseKind.User,
          TransformPhase.writeConfig(
            TransformPhaseKind.User, UserTransformConfig(class_name = "com.memsql.spark.interface.SlowTransformer", value = JsString("test")))),
        Phase[LoadPhaseKind](
          LoadPhaseKind.MemSQL,
          LoadPhase.writeConfig(
            LoadPhaseKind.MemSQL, MemSQLLoadConfig(dbName, "t4", None, None))))

      apiRef ? PipelinePut("pipeline4", single_step = None, batch_interval = Some(1), config = transformTestConfig)

      apiRef ! PipelineUpdate("pipeline4", trace_batch_count = Some(5))
      receiveOne(1.second).asInstanceOf[Try[Boolean]] match {
        case Success(resp) => assert(resp)
        case Failure(err) => fail(s"unexpected response $err")
      }

      whenReady((apiRef ? PipelineGet("pipeline4")).mapTo[Try[Pipeline]]) {
        case Success(pipeline) => {
          val pm = new DefaultPipelineMonitor(apiRef, pipeline, sc, streamingContext)
          pm.ensureStarted
          assert(pm.isAlive())

          Thread.sleep(2000)
          pm.stop
          val q = pipeline.metricsQueue

          var foundEvent = false
          while (!q.isEmpty) {
            val event = q.dequeue
            event.event_type match {
              case PipelineEventType.BatchCancelled => {
                foundEvent = true
                val cancelledEvent = event.asInstanceOf[BatchCancelledEvent]
                assert(cancelledEvent.pipeline_id == "pipeline4")
                assert(cancelledEvent.batch_type == PipelineBatchType.Traced)
              }
              case _ =>
            }
          }
          assert(foundEvent)
        }
        case Failure(error) => fail(s"Expected pipeline pipeline4 to exist: $error")
      }
    }
  }

  "PipelineMonitor" should {
    "terminate if the Pipeline is single-step" in {
      val singleStepConfig = PipelineConfig(
        Phase[ExtractPhaseKind](
          ExtractPhaseKind.TestLines,
          ExtractPhase.writeConfig(
            ExtractPhaseKind.TestLines, TestLinesExtractConfig("testtest\ntest\ntest\ntest\ntest\ntest"))),
        Phase[TransformPhaseKind](
          TransformPhaseKind.Identity,
          TransformPhase.writeConfig(
            TransformPhaseKind.Identity, IdentityTransformerConfig())),
        Phase[LoadPhaseKind](
          LoadPhaseKind.MemSQL,
          LoadPhase.writeConfig(
            LoadPhaseKind.MemSQL, MemSQLLoadConfig(dbName, "t5", None, None))))

      apiRef ? PipelinePut("pipeline5", single_step=Some(true), batch_interval=None, config=singleStepConfig)

      whenReady((apiRef ? PipelineGet("pipeline5")).mapTo[Try[Pipeline]]) {
        case Success(pipeline) => {
          val pm = new DefaultPipelineMonitor(apiRef, pipeline, sc, streamingContext)
          pm.ensureStarted
          assert(pm.isAlive)

          Thread.sleep(10000)

          whenReady((apiRef ? PipelineGet("pipeline5")).mapTo[Try[Pipeline]]) {
            case Success(pipeline) => {
              assert(pipeline.thread_state == PipelineThreadState.THREAD_STOPPED)
              assert(pipeline.state == PipelineState.FINISHED)

              val events = pipeline.metricsQueue
              assert(events.length == 4)
              assert(events(0).event_type == PipelineEventType.PipelineStart)
              assert(events(1).event_type == PipelineEventType.BatchStart)
              assert(events(2).event_type == PipelineEventType.BatchEnd)
              assert(events(3).event_type == PipelineEventType.PipelineEnd)
            }
            case Failure(error) => fail(s"Expected pipeline5 to exist: $error")
          }
        }
        case Failure(error) => fail(s"Expected pipeline5 to exist: $error")
      }
    }
  }

  "PipelineMonitor" should {
    "write to 'hasError' and 'error' if a single-step pipeline has a phase error" in {
      // ...implying that SparkInterface.checkPipeline() will set the PipelineState to ERROR

      val singleStepConfig = PipelineConfig(
        Phase[ExtractPhaseKind](
          ExtractPhaseKind.S3,
          ExtractPhase.writeConfig(
            ExtractPhaseKind.S3, S3ExtractConfig("", "", "", S3ExtractTaskConfig(""), None, None))),
        Phase[TransformPhaseKind](
          TransformPhaseKind.Identity,
          TransformPhase.writeConfig(
            TransformPhaseKind.Identity, IdentityTransformerConfig())),
        Phase[LoadPhaseKind](
          LoadPhaseKind.MemSQL,
          LoadPhase.writeConfig(
            LoadPhaseKind.MemSQL, MemSQLLoadConfig(dbName, "t6", None, None))))

      apiRef ? PipelinePut("pipeline6", single_step=Some(true), batch_interval=None, config=singleStepConfig)

      whenReady((apiRef ? PipelineGet("pipeline6")).mapTo[Try[Pipeline]]) {
        case Success(pipeline) => {
          val pm = new DefaultPipelineMonitor(apiRef, pipeline, sc, streamingContext)
          pm.ensureStarted
          assert(pm.isAlive)

          Thread.sleep(3000)

          assert(pm.hasError())
          assert(pm.error.isInstanceOf[S3ExtractException])

          whenReady((apiRef ? PipelineGet("pipeline6")).mapTo[Try[Pipeline]]) {
            case Success(pipeline) => {
              assert(pipeline.thread_state == PipelineThreadState.THREAD_STOPPED)
              assert(pipeline.state == PipelineState.RUNNING)

              val events = pipeline.metricsQueue
              assert(events.length == 4)
              assert(events(0).event_type == PipelineEventType.PipelineStart)
              assert(events(1).event_type == PipelineEventType.BatchStart)
              assert(events(2).event_type == PipelineEventType.BatchCancelled)
              assert(events(3).event_type == PipelineEventType.PipelineEnd)

            }
            case Failure(error) => fail(s"Expected pipeline6 to exist: $error")
          }
        }
        case Failure(error) => fail(s"Expected pipeline6 to exist: $error")
      }
    }
  }
}
