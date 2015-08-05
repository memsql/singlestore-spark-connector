package com.memsql.superapp

import akka.actor.{Actor, ActorRef, Props}
import com.memsql.spark.context.MemSQLSQLContext
import com.memsql.spark.etl.api.configs._
import ExtractPhaseKind._
import TransformPhaseKind._
import LoadPhaseKind._
import com.memsql.superapp.api._
import ApiActor._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}

class SuperAppSpec extends TestKitSpec("SuperAppSpec") {
  var mockTime = new MockTime()
  val apiRef = system.actorOf(Props(classOf[TestApiActor], mockTime))

  val config = PipelineConfig(
    Phase[ExtractPhaseKind](
      ExtractPhaseKind.Kafka,
      ExtractPhase.writeConfig(
        ExtractPhaseKind.Kafka, KafkaExtractConfig("test1", 9092, "topic", None))),
    Phase[TransformPhaseKind](
      TransformPhaseKind.Json,
      TransformPhase.writeConfig(
        TransformPhaseKind.Json, JsonTransformConfig())),
    Phase[LoadPhaseKind](
      LoadPhaseKind.MemSQL,
      LoadPhase.writeConfig(
        LoadPhaseKind.MemSQL, MemSQLLoadConfig("db", "table", None, None, None))))

  class MockPipelineMonitor(override val api: ActorRef,
                                         pipeline: Pipeline,
                            override val pipelineInstance: PipelineInstance[Any],
                            override val sparkContext: SparkContext,
                            override val streamingContext: StreamingContext,
                            override val sqlContext: SQLContext) extends PipelineMonitor {

    override def pipeline_id = pipeline.pipeline_id
    override def batchInterval = pipeline.batch_interval
    override def jar = pipeline.jar
    override def config = pipeline.config
    override def lastUpdated = pipeline.last_updated

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

  case object TestSuperApp extends Application {
    override lazy val config = Config(dataDir = "test_root")

    // use the TestKit actors
    override val system = SuperAppSpec.this.system
    override val api = apiRef
    override val web = null

    override val sparkConf = new SparkConf().setAppName("test").setMaster("local")

    override def newPipelineMonitor(pipeline: Pipeline) = {
      try {
        if (pipeline.pipeline_id == "fail") {
          throw new TestException("Pipeline monitor instantiation failed")
        }
        Success(new MockPipelineMonitor(api, pipeline, null, sparkContext, streamingContext, new MemSQLSQLContext(sparkContext)))
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

  def putPipeline(pipeline_id: String, jar: String, batch_interval: Long, config: PipelineConfig): Unit = {
    apiRef ! PipelinePut(pipeline_id, jar = jar, batch_interval = batch_interval, config = config)
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

  val superApp = TestSuperApp

  "SuperApp" should {
    "start a monitor for a new pipeline" in {
      putPipeline("pipeline1", jar = "site.com/buh.jar", batch_interval = 12000, config = config)

      var pipeline = getPipeline("pipeline1")
      assert(pipeline.pipeline_id == "pipeline1")
      assert(pipeline.state == PipelineState.RUNNING)
      assert(!superApp.pipelineMonitors.contains("pipeline1"))

      superApp.update

      val pipelineMonitor = superApp.pipelineMonitors.get(pipeline.pipeline_id).get
      assert(pipelineMonitor.pipeline_id == pipeline.pipeline_id)
      assert(pipelineMonitor.isAlive)
      pipeline = getPipeline("pipeline1")
      assert(pipeline.state == PipelineState.RUNNING)
    }

    "set pipeline to ERROR state if monitor failed on instantiation" in {
      putPipeline("fail", jar = "site.com/buh.jar", batch_interval = 10000, config = config)

      var pipeline = getPipeline("fail")
      assert(!superApp.pipelineMonitors.contains("fail"))

      superApp.update

      assert(!superApp.pipelineMonitors.contains("fail"))
      pipeline = getPipeline("fail")
      assert(pipeline.state == PipelineState.ERROR)
      assert(pipeline.error.get.contains("Pipeline monitor instantiation failed"))
    }

    "stop and remove the monitor for a pipeline in state STOPPED" in {
      var pipeline = getPipeline("pipeline1")
      assert(pipeline.pipeline_id == "pipeline1")
      assert(pipeline.state == PipelineState.RUNNING)

      apiRef ! PipelineUpdate("pipeline1", state = PipelineState.STOPPED)
      receiveOne(1.second).asInstanceOf[Try[Boolean]] match {
        case Success(resp) => assert(resp)
        case Failure(err) => fail(s"unexpected response $err")
      }

      superApp.update

      assert(!superApp.pipelineMonitors.contains(pipeline.pipeline_id))
      pipeline = getPipeline("pipeline1")
      assert(pipeline.state == PipelineState.STOPPED)
    }

    "restart monitors when they crash" in {
      apiRef ! PipelinePut("pipeline2", jar = "site.com/foo.jar", batch_interval = 1000, config = config)
      receiveOne(1.second) match {
        case Success(resp) =>
        case Failure(err) => fail(s"unexpected response $err")
      }

      var pipeline = getPipeline("pipeline2")
      assert(pipeline.pipeline_id == "pipeline2")
      assert(pipeline.state == PipelineState.RUNNING)

      superApp.update

      val pipelineMonitor = superApp.pipelineMonitors.get("pipeline2").get
      pipelineMonitor.stop
      assert(!pipelineMonitor.isAlive)

      pipeline = getPipeline("pipeline2")
      assert(pipeline.state == PipelineState.RUNNING)

      superApp.update

      //now monitor should be started again
      assert(pipelineMonitor.isAlive)
    }

    "clear monitors when pipelines are deleted" in {
      val pipelineMonitor = superApp.pipelineMonitors.get("pipeline2").get
      assert(pipelineMonitor.isAlive)

      val pipeline = getPipeline("pipeline2")
      assert(pipeline.state == PipelineState.RUNNING)

      apiRef ! PipelineDelete("pipeline2")
      receiveOne(1.second).asInstanceOf[Try[Boolean]] match {
        case Success(resp) => assert(resp)
        case Failure(err) => fail(s"unexpected response $err")
      }

      superApp.update

      //now monitor should be stopped and removed from pipelineMonitors
      assert(!superApp.pipelineMonitors.contains("pipeline2"))
      assert(!pipelineMonitor.isAlive)
    }

    "restart monitors when pipelines are updated" in {
      putPipeline("pipeline3", jar = "site.com/jar.jar", batch_interval = 1000, config = config)
      var pipeline = getPipeline("pipeline3")

      superApp.update

      var oldPipelineMonitor = superApp.pipelineMonitors.get("pipeline3").get
      assert(pipeline.pipeline_id == oldPipelineMonitor.pipeline_id)
      assert(oldPipelineMonitor.isAlive)
      assert(pipeline.jar == oldPipelineMonitor.jar)
      assert(pipeline.batch_interval == oldPipelineMonitor.batchInterval)
      assert(pipeline.config == oldPipelineMonitor.config)

      // after updating the batch interval, a new pipeline monitor should be running
      apiRef ! PipelineUpdate("pipeline3", batch_interval = Some(2000))
      receiveOne(1.second).asInstanceOf[Try[Boolean]] match {
        case Success(resp) => assert(resp)
        case Failure(err) => fail(s"unexpected response $err")
      }
      pipeline = getPipeline("pipeline3")
      superApp.update

      var newPipelineMonitor = superApp.pipelineMonitors.get("pipeline3").get
      assert(newPipelineMonitor != oldPipelineMonitor)
      assert(pipeline.pipeline_id == newPipelineMonitor.pipeline_id)
      assert(!oldPipelineMonitor.isAlive)
      assert(newPipelineMonitor.isAlive)
      assert(pipeline.jar == newPipelineMonitor.jar)
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
      superApp.update

      oldPipelineMonitor = newPipelineMonitor
      newPipelineMonitor = superApp.pipelineMonitors.get("pipeline3").get
      assert(newPipelineMonitor != oldPipelineMonitor)
      assert(pipeline.pipeline_id == newPipelineMonitor.pipeline_id)
      assert(!oldPipelineMonitor.isAlive)
      assert(newPipelineMonitor.isAlive)
      assert(pipeline.jar == newPipelineMonitor.jar)
      assert(pipeline.batch_interval == newPipelineMonitor.batchInterval)
      assert(pipeline.config == newPipelineMonitor.config)

      // updating the jar should always restart the pipeline monitor
      apiRef ! PipelineUpdate("pipeline3", jar = Some("site.com/jar.jar"))
      receiveOne(1.second).asInstanceOf[Try[Boolean]] match {
        case Success(resp) => assert(resp)
        case Failure(err) => fail(s"unexpected response $err")
      }
      pipeline = getPipeline("pipeline3")
      superApp.update

      oldPipelineMonitor = newPipelineMonitor
      newPipelineMonitor = superApp.pipelineMonitors.get("pipeline3").get
      assert(newPipelineMonitor != oldPipelineMonitor)
      assert(pipeline.pipeline_id == newPipelineMonitor.pipeline_id)
      assert(!oldPipelineMonitor.isAlive)
      assert(newPipelineMonitor.isAlive)
      // the jar is the same path as before, but we always restart
      assert(pipeline.jar == newPipelineMonitor.jar)
      assert(pipeline.jar == oldPipelineMonitor.jar)
      assert(pipeline.batch_interval == newPipelineMonitor.batchInterval)
      assert(pipeline.config == newPipelineMonitor.config)
    }
  }
}
