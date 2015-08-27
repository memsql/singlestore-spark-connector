package com.memsql.spark.interface

import com.memsql.spark.etl.utils.Logging
import com.memsql.spark.interface.api.{Pipeline, PipelineState, ApiActor}

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.io.IO
import com.memsql.spark.interface.server.WebServer
import ApiActor._
import com.memsql.spark.interface.util.Paths
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.ui.jobs.JobProgressListener
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Duration, StreamingContext}
import com.memsql.spark.context.MemSQLSparkContext
import spray.can.Http
import akka.pattern.ask
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Try, Success}
import scala.concurrent.ExecutionContext.Implicits.global

case class Config(port:Int = 10001,
                  dataDir:String = "",
                  dbHost:String = "127.0.0.1",
                  dbPort:Int = 3306,
                  dbUser:String = "root",
                  dbPassword:String = "",
                  debug:Boolean = false)

class SparkInterface(val providedConfig: Config) extends Application {
  override lazy val config = providedConfig
  override implicit val system = ActorSystem("spark-interface")
  override val api = system.actorOf(Props[ApiActor], "api")
  override val web = system.actorOf(Props[WebServer], "web")

  //TODO verify we have sane defaults for spark conf
  override val sparkConf = {
    new SparkConf().setAppName("MemSQL Spark Interface")
      .set("spark.blockManager.port", (config.port + 1).toString)
      .set("spark.broadcast.port", (config.port + 2).toString)
      .set("spark.driver.port", (config.port + 3).toString)
      .set("spark.executor.port", (config.port + 4).toString)
      .set("spark.fileserver.port", (config.port + 5).toString)
      .set("spark.ui.port", (config.port + 6).toString)
  }

  override val sparkContext = new MemSQLSparkContext(sparkConf, config.dbHost, config.dbPort, config.dbUser, config.dbPassword)
  override val streamingContext = new StreamingContext(sparkContext, new Duration(5000))
  override val jobProgressListener = new JobProgressListener(sparkConf)
  sparkContext.addSparkListener(jobProgressListener)

  // Use the sparkContext to get the master URL. This forces the sparkContext
  // be evaluated, ensuring that we attempt to connect to the master before
  // we start the HTTP API so that we don't respond to /ping requests if we
  // can't connect to the Spark master.
  val sparkMaster = sparkContext.master
  logInfo(s"Connected to a Spark master on $sparkMaster")

  IO(Http) ? Http.Bind(web, interface = "0.0.0.0", port = config.port) onComplete {
    case Success(Http.Bound(endpoint)) => logInfo(s"Listening on 0.0.0.0:${config.port}")
    case _ => {
      logError(s"Failed to bind to 0.0.0.0:${config.port}")
      system.shutdown()
      sys.exit(1)
    }
  }
}

trait Application extends Logging {
  val PIPELINE_UPDATE_INTERVAL = 5.seconds

  private[interface] lazy val config: Config = Config()
  Paths.initialize(config.dataDir)

  val loggingProperties = Logging.defaultProps
  loggingProperties.setProperty("log4j.logger.com.memsql", if (config.debug) "DEBUG" else "INFO")
  PropertyConfigurator.configure(loggingProperties)

  implicit private[interface] val system: ActorSystem
  private[interface] def api: ActorRef
  private[interface] def web: ActorRef

  private[interface] def sparkConf: SparkConf
  private[interface] def sparkContext: SparkContext
  private[interface] def streamingContext: StreamingContext
  private[interface] def jobProgressListener: JobProgressListener = null

  private[interface] var pipelineMonitors = Map[String, PipelineMonitor]()

  def run(): Unit = {
    while (true) {
      update()
      Thread.sleep(PIPELINE_UPDATE_INTERVAL.toMillis)
    }
  }

  def update(): Unit = {
    val pipelines = Await.result[List[Pipeline]]((api ? PipelineQuery).mapTo[List[Pipeline]], 5.seconds)
    pipelines.foreach(updatePipeline)
    cleanupPipelineMonitors(pipelines)
  }

  private[interface] def updatePipeline(pipeline: Pipeline): Unit = {
    (pipeline.state, pipelineMonitors.get(pipeline.pipeline_id)) match {
      //start a pipeline monitor for this pipeline
      case (PipelineState.RUNNING, None) => {
        startPipeline(pipeline)
      }
      //verify the monitor is still running and restart if necessary
      case (PipelineState.RUNNING, Some(pipelineMonitor)) => {
        pipeline.last_updated > pipelineMonitor.lastUpdated match {
          case true => {
            // NOTE: we always restart the pipeline monitor instead of mutating values in
            // the currently running monitor for simplicity
            stopPipeline(pipelineMonitor)
            startPipeline(pipeline)
          }
          case false => pipelineMonitor.ensureStarted
        }
      }
      //stop and remove the pipeline monitor if the user requested it
      case (PipelineState.STOPPED, Some(pipelineMonitor)) => {
        stopPipeline(pipelineMonitor)
      }
      case (_, _) => //do nothing
    }
  }

  private[interface] def startPipeline(pipeline: Pipeline): Unit = {
    newPipelineMonitor(pipeline) match {
      case Success(pipelineMonitor) => {
        pipelineMonitor.ensureStarted
        pipelineMonitors = pipelineMonitors + (pipeline.pipeline_id -> pipelineMonitor)
      }
      case Failure(error) => {
        val future = (api ? PipelineUpdate(pipeline.pipeline_id, PipelineState.ERROR, error = Some(error.toString))).mapTo[Try[Boolean]]
        future.map {
          case Success(resp) => //exit
          case Failure(error) => logError(s"Failed to update pipeline ${pipeline.pipeline_id} state to ERROR", error)
        }
      }
    }
  }

  private[interface] def stopPipeline(pipelineMonitor: PipelineMonitor): Unit = {
    pipelineMonitor.stop
    pipelineMonitors = pipelineMonitors.filterKeys(_ != pipelineMonitor.pipeline_id)
  }

  private[interface] def newPipelineMonitor(pipeline: Pipeline): Try[PipelineMonitor] = {
    try {
      Success(new DefaultPipelineMonitor(api, pipeline, sparkContext, streamingContext, jobProgressListener))
    } catch {
      case e: Exception => {
        val errorMessage = s"Failed to initialize pipeline ${pipeline.pipeline_id}"
        logError(errorMessage, e)
        Failure(e)
      }
    }
  }

  private[interface] def cleanupPipelineMonitors(pipelines: List[Pipeline]): Unit = {
    val currentPipelineIds = pipelines.map(_.pipeline_id).toSet
    val deletedPipelineIds = pipelineMonitors.flatMap((p: (String, PipelineMonitor)) => {
      val pipeline_id = p._1
      currentPipelineIds.contains(pipeline_id) match {
        case false => {
          val pipelineMonitor = p._2
          pipelineMonitor.stop
          Some(pipeline_id)
        }
        case true => None
      }
    })

    pipelineMonitors = pipelineMonitors -- deletedPipelineIds
  }
}
