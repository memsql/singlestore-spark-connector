package com.memsql.superapp

import java.io.File
import java.util.Calendar

import akka.pattern.ask
import akka.actor.Props
import akka.util.Timeout
import com.memsql.spark.context.{MemSQLSQLContext, MemSQLSparkContext}
import com.memsql.spark.etl.api.{Extractor, Transformer, Loader, ETLPipeline}
import com.memsql.superapp.api.{ApiActor, PipelineState, Pipeline}
import ApiActor._
import com.memsql.superapp.util.Paths
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType, StringType, StructField}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Time, Duration, StreamingContext}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}

class PipelineMonitorSpec extends TestKitSpec("PipelineMonitorSpec") with LocalMemSQLSparkContext {
  val apiRef = system.actorOf(Props(classOf[ApiActor], Config()))
  var sqlContext: MemSQLSQLContext = _
  var streamingContext: StreamingContext = _
  implicit val timeout = Timeout(5.seconds)

  override def beforeEach(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    sc = new MemSQLSparkContext(conf, "127.0.0.1", 3306, "root", "")
    sqlContext = new MemSQLSQLContext(sc)
    streamingContext = new StreamingContext(sc, new Duration(5000))
  }

  "PipelineMonitor" should {
    "create a monitor if the class can be properly loaded" in {
      val jarPath = Paths.join(new File(".").getCanonicalPath, "target/scala-2.10/MemSQL-assembly-0.1.2.jar")
      apiRef ? PipelinePut("pipeline2", jarPath, "com.memsql.spark.examples.MemSQLETLApp")
      whenReady((apiRef ? PipelineGet("pipeline2")).mapTo[Try[Pipeline]]) {
        case Success(pipeline) => {
          val maybePipelineMonitor = PipelineMonitor.of(apiRef, pipeline, sc, sqlContext, streamingContext)
          assert(maybePipelineMonitor.isDefined)
          val pm = maybePipelineMonitor.get
          assert(pm.pipeline_id == "pipeline2")
          assert(!pm.isAlive)
          assert(pipeline.state == PipelineState.RUNNING)
        }
        case Failure(error) => fail(s"Expected pipeline pipeline2 to exist: $error")
      }
    }

    "fail to create a monitor if the class cannot be loaded" in {
      //create pipeline and try to load in a PipelineMonitor
      apiRef ! PipelinePut("pipeline1", "file://doesnt_exist.jar", "com.memsql.SomethingETL")
      whenReady((apiRef ? PipelineGet("pipeline1")).mapTo[Try[Pipeline]]) {
        case Success(pipeline) => {
          PipelineMonitor.of(apiRef, pipeline, sc, sqlContext, streamingContext) shouldBe None
          whenReady((apiRef ? PipelineGet("pipeline1")).mapTo[Try[Pipeline]]) {
            case Success(updatedPipeline) => {
              assert(updatedPipeline.state == PipelineState.ERROR)
              assert(updatedPipeline.error.get.contains("Could not load"))
            }
            case Failure(error) => fail(s"Expected pipeline ${pipeline.pipeline_id} to exist: $error")
          }
        }
        case Failure(error) => fail(s"Expected pipeline pipeline1 to exist: $error")
      }
    }
  }
}
