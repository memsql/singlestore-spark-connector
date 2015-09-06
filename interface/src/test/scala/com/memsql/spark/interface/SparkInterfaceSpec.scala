package com.memsql.spark.interface

import akka.actor.{ActorRef, Props}
import com.memsql.spark.etl.api.configs._
import ExtractPhaseKind._
import TransformPhaseKind._
import LoadPhaseKind._
import com.memsql.spark.interface.api.{PipelineInstance, Pipeline, PipelineState, ApiActor}
import ApiActor._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{StreamingContext, Duration}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}

class SparkInterfaceSpec extends TestKitSpec("SparkInterfaceSpec") with LocalSparkContext {
  var mockTime = new MockTime()
  val apiRef = system.actorOf(Props(classOf[TestApiActor], mockTime))

  override def beforeEach(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    sc = new SparkContext(conf)
  }

  val config = PipelineConfig(
    Phase[ExtractPhaseKind](
      ExtractPhaseKind.Kafka,
      ExtractPhase.writeConfig(
        ExtractPhaseKind.Kafka, KafkaExtractConfig("test1", 9092, "topic"))),
    Phase[TransformPhaseKind](
      TransformPhaseKind.Json,
      TransformPhase.writeConfig(
        TransformPhaseKind.Json, JsonTransformConfig("data"))),
    Phase[LoadPhaseKind](
      LoadPhaseKind.MemSQL,
      LoadPhase.writeConfig(
        LoadPhaseKind.MemSQL, MemSQLLoadConfig("db", "table", None, None))))

  class MockPipelineMonitor(override val api: ActorRef,
                                         pipeline: Pipeline,
                            override val pipelineInstance: PipelineInstance,
                            override val sparkContext: SparkContext,
                            override val streamingContext: StreamingContext,
                            override val sqlContext: SQLContext) extends PipelineMonitor {

    override def pipeline_id = pipeline.pipeline_id
    override def batchInterval = pipeline.batch_interval
    override def config = pipeline.config
    override def lastUpdated = pipeline.last_updated
    override def hasError: Boolean = error != null
    override var error: Throwable = null

    var running = false

    override def runPipeline(): Unit = {}

    override def ensureStarted(): Unit = {
      running = true
    }

    override def isAlive: Boolean = running

    override def stop(): Unit = {
      running match {
        case false => fail("pipeline is not running")
        case true => running = false
      }
    }
  }

  case object TestSparkInterface extends Application {
    override lazy val config = Config(dataDir = "test_root")

    // use the TestKit actors
    override val system = SparkInterfaceSpec.this.system
    override def api = apiRef
    override def web = null

    override def sparkConf = new SparkConf().setAppName("test").setMaster("local")
    override def sparkContext: SparkContext = sc
    override def streamingContext: StreamingContext = new StreamingContext(sc, new Duration(5000))

    override def newPipelineMonitor(pipeline: Pipeline) = {
      try {
        if (pipeline.pipeline_id == "fail") {
          throw new TestException("Pipeline monitor instantiation failed")
        }
        Success(new MockPipelineMonitor(api, pipeline, null, sparkContext, streamingContext, new SQLContext(sparkContext)))
      } catch {
        case e: Exception => {
          {
            val errorMessage = s"Failed to initialize pipeline ${pipeline.pipeline_id}: $e"
            Console.println(errorMessage)
            e.printStackTrace
            Failure(e)
          }
        }
      }
    }

    override def update = {
      super.update()
      mockTime.tick()
    }
  }

  def putPipeline(pipeline_id: String, batch_interval: Long, config: PipelineConfig): Unit = {
    apiRef ! PipelinePut(pipeline_id, batch_interval = batch_interval, config = config)
    receiveOne(1.second).asInstanceOf[Try[Boolean]] match {
      case Success(resp) => assert(resp)
      case Failure(err) => fail(s"unexpected response $err")
    }
  }

  def getPipeline(pipeline_id: String): Pipeline = {
    apiRef ! PipelineGet(pipeline_id)
    receiveOne(1.second) match {
      case resp: Success[_] => {
        {
          resp.get.asInstanceOf[Pipeline]
        }
      }
      case Failure(err) => fail(s"unexpected response $err")
    }
  }

  val sparkInterface = TestSparkInterface

  "SparkInterface" should {
    "start a monitor for a new pipeline" in {
      putPipeline("pipeline1", batch_interval = 12000, config = config)

      var pipeline = getPipeline("pipeline1")
      assert(pipeline.pipeline_id == "pipeline1")
      assert(pipeline.state == PipelineState.RUNNING)
      assert(!sparkInterface.pipelineMonitors.contains("pipeline1"))

      sparkInterface.update

      val pipelineMonitor = sparkInterface.pipelineMonitors.get(pipeline.pipeline_id).get
      assert(pipelineMonitor.pipeline_id == pipeline.pipeline_id)
      assert(pipelineMonitor.isAlive)
      pipeline = getPipeline("pipeline1")
      assert(pipeline.state == PipelineState.RUNNING)
    }

    "set pipeline to ERROR state if monitor failed on instantiation" in {
      putPipeline("fail", batch_interval = 10000, config = config)

      var pipeline = getPipeline("fail")
      assert(!sparkInterface.pipelineMonitors.contains("fail"))

      sparkInterface.update

      assert(!sparkInterface.pipelineMonitors.contains("fail"))
      pipeline = getPipeline("fail")
      assert(pipeline.state == PipelineState.ERROR)
      assert(pipeline.error.get.contains("Pipeline monitor instantiation failed"))
    }

    "stop and remove the monitor for a pipeline in state STOPPED" in {
      var pipeline = getPipeline("pipeline1")
      assert(pipeline.pipeline_id == "pipeline1")
      assert(pipeline.state == PipelineState.RUNNING)

      apiRef ! PipelineUpdate("pipeline1", state = Some(PipelineState.STOPPED))
      receiveOne(1.second).asInstanceOf[Try[Boolean]] match {
        case Success(resp) => assert(resp)
        case Failure(err) => fail(s"unexpected response $err")
      }

      sparkInterface.update

      assert(!sparkInterface.pipelineMonitors.contains(pipeline.pipeline_id))
      pipeline = getPipeline("pipeline1")
      assert(pipeline.state == PipelineState.STOPPED)
    }

    "stop and remove the monitor for a pipeline in state ERROR" in {
      putPipeline("pipeline4", batch_interval = 12000, config = config)
      var pipeline = getPipeline("pipeline4")
      assert(pipeline.pipeline_id == "pipeline4")
      assert(pipeline.state == PipelineState.RUNNING)

      sparkInterface.update

      val pipelineMonitor = sparkInterface.pipelineMonitors.get(pipeline.pipeline_id).get
      assert(pipelineMonitor.isAlive)

      pipelineMonitor.error = new Exception("could not connect to kafka")
      sparkInterface.update

      assert(!sparkInterface.pipelineMonitors.contains(pipeline.pipeline_id))
      assert(!pipelineMonitor.isAlive)
      pipeline = getPipeline("pipeline4")
      assert(pipeline.error.get.contains("java.lang.Exception: could not connect to kafka"))
      assert(pipeline.state == PipelineState.ERROR)
    }

    "restart monitors when they crash" in {
      apiRef ! PipelinePut("pipeline2", batch_interval = 1000, config = config)
      receiveOne(1.second) match {
        case Success(resp) =>
        case Failure(err) => fail(s"unexpected response $err")
      }

      var pipeline = getPipeline("pipeline2")
      assert(pipeline.pipeline_id == "pipeline2")
      assert(pipeline.state == PipelineState.RUNNING)

      sparkInterface.update

      val pipelineMonitor = sparkInterface.pipelineMonitors.get("pipeline2").get
      pipelineMonitor.stop
      assert(!pipelineMonitor.isAlive)

      pipeline = getPipeline("pipeline2")
      assert(pipeline.state == PipelineState.RUNNING)

      sparkInterface.update

      //now monitor should be started again
      assert(pipelineMonitor.isAlive)
    }

    "clear monitors when pipelines are deleted" in {
      val pipelineMonitor = sparkInterface.pipelineMonitors.get("pipeline2").get
      assert(pipelineMonitor.isAlive)

      val pipeline = getPipeline("pipeline2")
      assert(pipeline.state == PipelineState.RUNNING)

      apiRef ! PipelineDelete("pipeline2")
      receiveOne(1.second).asInstanceOf[Try[Boolean]] match {
        case Success(resp) => assert(resp)
        case Failure(err) => fail(s"unexpected response $err")
      }

      sparkInterface.update

      //now monitor should be stopped and removed from pipelineMonitors
      assert(!sparkInterface.pipelineMonitors.contains("pipeline2"))
      assert(!pipelineMonitor.isAlive)
    }

    "restart monitors when pipelines are updated" in {
      putPipeline("pipeline3", batch_interval = 1000, config = config)
      var pipeline = getPipeline("pipeline3")

      sparkInterface.update

      var oldPipelineMonitor = sparkInterface.pipelineMonitors.get("pipeline3").get
      assert(pipeline.pipeline_id == oldPipelineMonitor.pipeline_id)
      assert(oldPipelineMonitor.isAlive)
      assert(pipeline.batch_interval == oldPipelineMonitor.batchInterval)
      assert(pipeline.config == oldPipelineMonitor.config)

      // after updating the batch interval, a new pipeline monitor should be running
      apiRef ! PipelineUpdate("pipeline3", batch_interval = Some(2000))
      receiveOne(1.second).asInstanceOf[Try[Boolean]] match {
        case Success(resp) => assert(resp)
        case Failure(err) => fail(s"unexpected response $err")
      }
      pipeline = getPipeline("pipeline3")
      sparkInterface.update

      var newPipelineMonitor = sparkInterface.pipelineMonitors.get("pipeline3").get
      assert(newPipelineMonitor != oldPipelineMonitor)
      assert(pipeline.pipeline_id == newPipelineMonitor.pipeline_id)
      assert(!oldPipelineMonitor.isAlive)
      assert(newPipelineMonitor.isAlive)
      assert(pipeline.batch_interval == newPipelineMonitor.batchInterval)
      assert(pipeline.config == newPipelineMonitor.config)

      // updating the config should also restart the pipeline monitor
      val newConfig = config.copy(extract = Phase[ExtractPhaseKind](
        ExtractPhaseKind.User,
        ExtractPhase.writeConfig(
          ExtractPhaseKind.User,
          UserExtractConfig("com.foobar.Extract", ""))))

      apiRef ! PipelineUpdate("pipeline3", config = Some(newConfig))
      receiveOne(1.second).asInstanceOf[Try[Boolean]] match {
        case Success(resp) => assert(resp)
        case Failure(err) => fail(s"unexpected response $err")
      }
      pipeline = getPipeline("pipeline3")
      sparkInterface.update

      oldPipelineMonitor = newPipelineMonitor
      newPipelineMonitor = sparkInterface.pipelineMonitors.get("pipeline3").get
      assert(newPipelineMonitor != oldPipelineMonitor)
      assert(pipeline.pipeline_id == newPipelineMonitor.pipeline_id)
      assert(!oldPipelineMonitor.isAlive)
      assert(newPipelineMonitor.isAlive)
      assert(pipeline.batch_interval == newPipelineMonitor.batchInterval)
      assert(pipeline.config == newPipelineMonitor.config)

      // updating the config with the same blob should not restart the pipeline monitor
      apiRef ! PipelineUpdate("pipeline3", config = Some(newConfig))
      receiveOne(1.second).asInstanceOf[Try[Boolean]] match {
        case Success(resp) => assert(!resp)
        case Failure(err) => fail(s"unexpected response $err")
      }
      pipeline = getPipeline("pipeline3")
      sparkInterface.update

      oldPipelineMonitor = newPipelineMonitor
      newPipelineMonitor = sparkInterface.pipelineMonitors.get("pipeline3").get
      assert(newPipelineMonitor == oldPipelineMonitor)
      assert(newPipelineMonitor.isAlive)
      assert(newPipelineMonitor.config == newConfig)
    }
  }
}
