// scalastyle:off magic.number regex

package com.memsql.spark.interface

import akka.actor.{ActorRef, Props}
import com.memsql.spark.etl.api.configs.ExtractPhaseKind.ExtractPhaseKind
import com.memsql.spark.etl.api.configs.LoadPhaseKind.LoadPhaseKind
import com.memsql.spark.etl.api.configs.TransformPhaseKind.TransformPhaseKind
import com.memsql.spark.etl.api.configs._
import com.memsql.spark.interface.api.ApiActor.{PipelineProgress, PipelinePut}
import com.memsql.spark.interface.api.SparkProgressInfo
import com.memsql.spark.phases.configs.ExtractPhase
import com.memsql.spark.phases.{IdentityTransformerConfig, S3ExtractConfig, S3ExtractTaskConfig, S3Extractor}
import org.apache.spark.sql.memsql.test.TestBase
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class SparkProgressSpec extends TestKitSpec("ProgressSpec") with TestBase with BeforeAndAfterAll {

  // Use an S3Extractor to test SparkProgress
  val extractor = new S3Extractor

  val sparkConf = new SparkConf()
    .setAppName("test")
    .setMaster("local")
    .set("memsql.host", masterConnectionInfo.dbHost)
    .set("memsql.port", masterConnectionInfo.dbPort.toString)
    .set("memsql.user", masterConnectionInfo.user)
    .set("memsql.password", masterConnectionInfo.password)
    .set("memsql.defaultDatabase", masterConnectionInfo.dbName)

  val sparkProgressListener = new SparkProgressListener(sparkConf)
  val sparkProgress = sparkProgressListener.sparkProgress
  val mockTime = new MockTime()
  val apiRef = system.actorOf(Props(classOf[TestApiActor], mockTime, sparkProgress))

  override def beforeAll(): Unit = {
    super.beforeAll()

    recreateDatabase
    sc = new SparkContext(sparkConf)
    sc.addSparkListener(sparkProgressListener)
  }

  override def afterAll(): Unit = {
    sparkDown
    super.afterAll()
  }

  case object TestSparkInterface extends Application {
    override lazy val config = Config(dataDir = "test_root")

    override val system = SparkProgressSpec.this.system
    override def api: ActorRef = apiRef
    override def web: ActorRef = null

    override def sparkConf: SparkConf = SparkProgressSpec.this.sparkConf
    override def sparkContext: SparkContext = sc
    override def streamingContext: StreamingContext = new StreamingContext(sc, new Duration(5000))
  }

  val sparkInterface = TestSparkInterface

  def testExtractConfig(key: String): S3ExtractConfig = {
    val awsAccessKeyID = sys.env("AWS_ACCESS_KEY_ID_TEST")
    val awsSecretAccessKey = sys.env("AWS_SECRET_ACCESS_KEY_TEST")
    val bucket = "loader-tests"

    val taskConfig = S3ExtractTaskConfig(key)
    S3ExtractConfig(awsAccessKeyID, awsSecretAccessKey, bucket, taskConfig, None, None)
  }

  val testPipelineConfig = PipelineConfig(
    Phase[ExtractPhaseKind](
      ExtractPhaseKind.S3,
      ExtractPhase.writeConfig(
        ExtractPhaseKind.S3, testExtractConfig("big_files/file1"))),
    Phase[TransformPhaseKind](
      TransformPhaseKind.Identity,
      TransformPhase.writeConfig(
        TransformPhaseKind.Identity, IdentityTransformerConfig())),
    Phase[LoadPhaseKind](
      LoadPhaseKind.MemSQL,
      LoadPhase.writeConfig(
        LoadPhaseKind.MemSQL, MemSQLLoadConfig(dbName, "t", None, None))))


  "SparkProgress" should {
    "store up-to-date progress state" in {
      // create a pipeline
      apiRef ! PipelinePut("p0", Some(true), None, None, testPipelineConfig)
      receiveOne(1.second).asInstanceOf[Try[Boolean]] match {
        case Success(resp) => assert(resp)
        case Failure(err) => fail(s"unexpected response $err")
      }

      // no progress state yet
      apiRef ! PipelineProgress("p0")
      receiveOne(1.second).asInstanceOf[Try[SparkProgressInfo]] match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.getMessage == "no progress messages associated with pipeline id p0")
      }

      // start the pipeline
      sparkInterface.update()
      Thread.sleep(10 * 1000)

      // check progress metrics
      apiRef ! PipelineProgress("p0")
      receiveOne(1.second).asInstanceOf[Try[SparkProgressInfo]] match {
        case Success(resp) => {
          assert(resp.totalTasks == 2)
          assert(resp.tasksFailed == 0)
          assert(0 <= resp.tasksSucceeded && resp.tasksSucceeded <= 2)

          // the timing is unpredictable - we might catch any of these intermediate progress states,
          // and any of them are valid
          resp.tasksSucceeded match {
            case 0 => {
              assert(resp.bytesRead == 0)
              assert(resp.recordsRead == 0)
            }
            case 1 => {
              assert(resp.bytesRead == 67108864 || resp.bytesRead == 14516570)
            }
            case 2 => {
              assert(resp.bytesRead == 81625434)
              assert(resp.recordsRead == 300000)
            }
            case _ => assert(false)
          }
        }
        case Failure(err) => fail(s"unexpected response $err")
      }

      // let the pipeline finish
      Thread.sleep(20 * 1000)
      sparkInterface.update()

      // check progress metrics
      apiRef ! PipelineProgress("p0")
      receiveOne(1.second).asInstanceOf[Try[SparkProgressInfo]] match {
        case Success(resp) => {
          assert(resp.bytesRead == 81625434)
          assert(resp.recordsRead == 300000)
          assert(resp.totalTasks == 2)
          assert(resp.tasksSucceeded == 2)
          assert(resp.tasksFailed == 0)
        }
        case Failure(err) => fail(s"unexpected response $err")
      }
    }

    "handle two concurrent pipelines" in {
      // create two pipelines
      apiRef ! PipelinePut("p1", Some(true), None, None, testPipelineConfig)
      receiveOne(1.second).asInstanceOf[Try[Boolean]] match {
        case Success(resp) => assert(resp)
        case Failure(err) => fail(s"unexpected response $err")
      }

      apiRef ! PipelinePut("p2", Some(true), None, None, testPipelineConfig)
      receiveOne(1.second).asInstanceOf[Try[Boolean]] match {
        case Success(resp) => assert(resp)
        case Failure(err) => fail(s"unexpected response $err")
      }

      // run both pipelines to completion
      sparkInterface.update()
      Thread.sleep(60 * 1000)
      sparkInterface.update()

      // check progress metrics
      apiRef ! PipelineProgress("p1")
      receiveOne(1.second).asInstanceOf[Try[SparkProgressInfo]] match {
        case Success(resp) => {
          assert(resp.bytesRead == 81625434)
          assert(resp.recordsRead == 300000)
          assert(resp.totalTasks == 2)
          assert(resp.tasksSucceeded == 2)
          assert(resp.tasksFailed == 0)
        }
        case Failure(err) => fail(s"unexpected response $err")
      }

      apiRef ! PipelineProgress("p2")
      receiveOne(1.second).asInstanceOf[Try[SparkProgressInfo]] match {
        case Success(resp) => {
          assert(resp.bytesRead == 81625434)
          assert(resp.recordsRead == 300000)
          assert(resp.totalTasks == 2)
          assert(resp.tasksSucceeded == 2)
          assert(resp.tasksFailed == 0)
        }
        case Failure(err) => fail(s"unexpected response $err")
      }
    }
  }
}
