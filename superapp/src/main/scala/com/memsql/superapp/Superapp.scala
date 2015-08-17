package com.memsql.superapp

import com.memsql.spark.etl.utils.Logging
import com.memsql.superapp.server.WebServer

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.io.IO
import com.memsql.superapp.util.Paths
import com.memsql.superapp.api._
import ApiActor._
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
                  dbPassword:String = "")

object SuperApp {
  val VERSION = "0.1.3"

  val PIPELINE_UPDATE_INTERVAL = 5.seconds

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("superapp") {
      override def showUsageOnError = true

      head("superapp", VERSION)
      opt[Int]('P', "port") action { (x, c) => c.copy(port = x) } text "Superapp port"
      opt[String]('D', "dataDir") required() action { (x, c) => c.copy(dataDir = x) } text "Superapp data directory"

      opt[String]("dbHost") action { (x, c) => c.copy(dbHost = x) } text "MemSQL Master host"
      opt[Int]("dbPort") action { (x, c) => c.copy(dbPort = x) } text "MemSQL Master port"
      opt[String]("dbUser") action { (x, c) => c.copy(dbUser = x) } text "MemSQL Master user"
      opt[String]("dbPassword") action { (x, c) => c.copy(dbPassword = x) } text "MemSQL Master password"
    }

    parser.parse(args, Config()) match {
      case Some(config) => {
        new SuperApp(config).run()
      }
      case None => sys.exit(1)
    }
  }
}

class SuperApp(val providedConfig: Config) extends Application {
  override lazy val config = providedConfig
  override implicit val system = ActorSystem("superapp")
  override val api = system.actorOf(Props[ApiActor], "api")
  override val web = system.actorOf(Props[WebServer], "web")

  //TODO verify we have sane defaults for spark conf
  override val sparkConf = {
    new SparkConf().setAppName("SuperApp Manager")
      .set("spark.blockManager.port", (config.port + 1).toString)
      .set("spark.broadcast.port", (config.port + 2).toString)
      .set("spark.driver.port", (config.port + 3).toString)
      .set("spark.executor.port", (config.port + 4).toString)
      .set("spark.fileserver.port", (config.port + 5).toString)
  }

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
  private[superapp] lazy val config: Config = Config()
  Paths.initialize(config.dataDir)

  implicit private[superapp] val system: ActorSystem
  private[superapp] val api: ActorRef
  private[superapp] val web: ActorRef

  private[superapp] val sparkConf: SparkConf
  private[superapp] lazy val sparkContext: SparkContext = new MemSQLSparkContext(sparkConf, config.dbHost, config.dbPort, config.dbUser, config.dbPassword)
  private[superapp] lazy val streamingContext = new StreamingContext(sparkContext, new Duration(5000))

  private[superapp] var pipelineMonitors = Map[String, PipelineMonitor]()

  def run(): Unit = {
    while (true) {
      update()
      Thread.sleep(SuperApp.PIPELINE_UPDATE_INTERVAL.toMillis)
    }
  }

  def update(): Unit = {
    val pipelines = Await.result[List[Pipeline]]((api ? PipelineQuery).mapTo[List[Pipeline]], 5.seconds)
    pipelines.foreach(updatePipeline)
    cleanupPipelineMonitors(pipelines)
  }

  private[superapp] def updatePipeline(pipeline: Pipeline): Unit = {
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

  private[superapp] def startPipeline(pipeline: Pipeline): Unit = {
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

  private[superapp] def stopPipeline(pipelineMonitor: PipelineMonitor): Unit = {
    pipelineMonitor.stop
    pipelineMonitors = pipelineMonitors.filterKeys(_ != pipelineMonitor.pipeline_id)
  }

  private[superapp] def newPipelineMonitor(pipeline: Pipeline): Try[PipelineMonitor] = {
    try {
      Success(new DefaultPipelineMonitor(api, pipeline, sparkContext, streamingContext))
    } catch {
      case e: Exception => {
        val errorMessage = s"Failed to initialize pipeline ${pipeline.pipeline_id}"
        logError(errorMessage, e)
        Failure(e)
      }
    }
  }

  private[superapp] def cleanupPipelineMonitors(pipelines: List[Pipeline]): Unit = {
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
