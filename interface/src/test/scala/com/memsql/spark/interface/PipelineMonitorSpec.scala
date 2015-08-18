package com.memsql.spark.interface

import java.io.File

import akka.pattern.ask
import akka.actor.Props
import akka.util.Timeout
import com.memsql.spark.etl.api.configs._
import ExtractPhaseKind._
import TransformPhaseKind._
import LoadPhaseKind._
import com.memsql.spark.interface.api.{Pipeline, PipelineState, ApiActor}
import ApiActor._
import com.memsql.spark.interface.util.{Paths, JarLoaderException}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}

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

  val config = PipelineConfig(
    Phase[ExtractPhaseKind](
      ExtractPhaseKind.Kafka,
      ExtractPhase.writeConfig(
        ExtractPhaseKind.Kafka, KafkaExtractConfig("test", 9092, "topic", None))),
    Phase[TransformPhaseKind](
      TransformPhaseKind.Json,
      TransformPhase.writeConfig(
        TransformPhaseKind.Json, JsonTransformConfig())),
    Phase[LoadPhaseKind](
      LoadPhaseKind.MemSQL,
      LoadPhase.writeConfig(
        LoadPhaseKind.MemSQL, MemSQLLoadConfig("db", "table", None, None, None))))
  val localJarFile = s"target/scala-2.10/MemSQL-assembly-${Main.VERSION}.jar"

  "PipelineMonitor" should {
    "create a monitor if the class can be properly loaded" in {
      val jarPath = Paths.join(new File(".").getCanonicalPath, localJarFile)
      apiRef ? PipelinePut("pipeline2", jar=jarPath, batch_interval=10, config=config)
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
          ExtractPhaseKind.User, UserExtractConfig("com.test.Extract", ""))))

      //create pipeline which requires loading a class from the jar and try to load in a PipelineMonitor
      apiRef ! PipelinePut("pipeline1", jar="file://doesnt_exist.jar", batch_interval=100, config=config2)
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
}
