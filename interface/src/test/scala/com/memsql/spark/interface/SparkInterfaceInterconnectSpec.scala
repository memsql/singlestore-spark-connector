package com.memsql.spark.interface

import akka.actor.{ActorRef, Props}
import com.memsql.spark.etl.api.configs._
import com.memsql.spark.etl.api.configs.ExtractPhaseKind._
import com.memsql.spark.etl.api.configs.LoadPhaseKind._
import com.memsql.spark.etl.api.configs.TransformPhaseKind._
import com.memsql.spark.interface.api.ApiActor.{PipelineUpdate, PipelineGet, PipelinePut}
import com.memsql.spark.interface.api.PipelineState.PipelineState
import com.memsql.spark.interface.api.PipelineThreadState
import com.memsql.spark.interface.api.PipelineThreadState.PipelineThreadState
import com.memsql.spark.interface.api.{PipelineThreadState, PipelineState, Pipeline}
import com.memsql.spark.interface.api.PipelineState.PipelineState
import com.memsql.spark.phases.{JsonTransformConfig, ZookeeperManagedKafkaExtractConfig}
import com.memsql.spark.phases.configs.ExtractPhase
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class SparkInterfaceInterconnectSpec extends TestKitSpec("SparkInterfaceSpec") with LocalSparkContext {
  var mockTime = new MockTime()
  val apiRef = system.actorOf(Props(classOf[TestApiActor], mockTime))

  val DURATION = 5000
  val BATCH_INTERVAL = 12000

  override def beforeEach(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    sc = new SparkContext(conf)
  }

  val pipelineConfig = PipelineConfig(
    Phase[ExtractPhaseKind](
      ExtractPhaseKind.ZookeeperManagedKafka,
      ExtractPhase.writeConfig(
        ExtractPhaseKind.ZookeeperManagedKafka, ZookeeperManagedKafkaExtractConfig(List("test1:2181"), "topic"))),
    Phase[TransformPhaseKind](
      TransformPhaseKind.Json,
      TransformPhase.writeConfig(
        TransformPhaseKind.Json, JsonTransformConfig("data"))),
    Phase[LoadPhaseKind](
      LoadPhaseKind.MemSQL,
      LoadPhase.writeConfig(
        LoadPhaseKind.MemSQL, MemSQLLoadConfig("db", "table", None, None))))

  case object InterconnectTest extends Application {
    override lazy val config = Config(dataDir = "test_root")

    // use the TestKit actors
    override val system = SparkInterfaceInterconnectSpec.this.system
    override def api: ActorRef = apiRef
    override def web: ActorRef = null

    override def sparkConf: SparkConf = new SparkConf().setAppName("test").setMaster("local")
    override def sparkContext: SparkContext = sc
    override def streamingContext: StreamingContext = new StreamingContext(sc, new Duration(DURATION))
  }

  def putPipeline(pipeline_id: String, batch_interval: Long, config: PipelineConfig): Unit = {
    apiRef ! PipelinePut(pipeline_id, batch_interval = batch_interval, config = config)
    receiveOne(1.second).asInstanceOf[Try[Boolean]] match {
      case Success(resp) => assert(resp)
      case Failure(err) => fail(s"unexpected response $err")
    }
  }

  def updatePipeline(pipeline_id: String, state: PipelineState,
                     batch_interval: Long,
                     config: PipelineConfig,
                     trace_batch_count: Int,
                     threadState: PipelineThreadState): Unit = {
    apiRef ! PipelineUpdate(
      pipeline_id, Some(state), batch_interval = Some(batch_interval),
      config = Some(config), trace_batch_count = Some(trace_batch_count),
      threadState = threadState)
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

  val sparkInterface = InterconnectTest

  "SparkInterface" should {
    "set thread_state for a new pipeline" in {
      putPipeline("running_pipeline", batch_interval = BATCH_INTERVAL, config = pipelineConfig)
      sparkInterface.update
      val pipelineInitial = getPipeline("running_pipeline")
      val pipelineMonitor = sparkInterface.pipelineMonitors.get(pipelineInitial.pipeline_id)
      sparkInterface.update
      val pipelineRunning = getPipeline("running_pipeline")
      assert(pipelineRunning.thread_state == PipelineThreadState.THREAD_RUNNING)
      assert(pipelineRunning.state == PipelineState.RUNNING)

      updatePipeline(pipeline_id = "running_pipeline", state = PipelineState.STOPPED,
        batch_interval = pipelineRunning.batch_interval, config = pipelineRunning.config,
        trace_batch_count = 0, threadState = PipelineThreadState.THREAD_STOPPED)
      sparkInterface.update
      val pipelineFinal = getPipeline("running_pipeline")
      assert(pipelineFinal.thread_state == PipelineThreadState.THREAD_STOPPED)
      pipelineMonitor.get.stop
    }
  }
}
