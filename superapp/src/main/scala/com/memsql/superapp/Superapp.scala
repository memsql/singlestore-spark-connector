package com.memsql.superapp

import com.memsql.superapp.server.WebServer

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import com.memsql.superapp.util.Paths
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}
import com.memsql.spark.context.{MemSQLSparkContext, MemSQLSQLContext}
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
                  dbName:String = "memsql_spark")

object SuperApp {
  val VERSION = "0.1.3"

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
      opt[String]("dbName") action { (x, c) => c.copy(dbName = x) } text "MemSQL Master name"
    }

    parser.parse(args, Config()) match {
      case Some(config) => run(config)
      case None => sys.exit(1)
    }
  }

  def init(config:Config): Unit = {
    Paths.initialize(config.dataDir)
  }

  def run(config:Config) {
    import com.memsql.superapp.api._
    import com.memsql.superapp.api.ApiActor._

    init(config)

    //TODO verify we have sane defaults for spark conf
    val sparkConf = new SparkConf().setAppName("SuperApp Manager")
      .set("spark.blockManager.port", (config.port + 1).toString)
      .set("spark.broadcast.port", (config.port + 2).toString)
      .set("spark.driver.port", (config.port + 3).toString)
      .set("spark.executor.port", (config.port + 4).toString)
      .set("spark.fileserver.port", (config.port + 5).toString)
    val sparkContext = new MemSQLSparkContext(sparkConf, config.dbHost, config.dbPort, config.dbUser, config.dbPassword)
    val sqlContext = new MemSQLSQLContext(sparkContext)
    val sparkStreamingContext = new StreamingContext(sparkContext, new Duration(5000))

    implicit val system = ActorSystem("superapp")
    var pipelineMonitors = Map[String, PipelineMonitor]()

    // initialize api
    import ApiActor._
    val api = system.actorOf(Props(classOf[ApiActor], config), "api")

    // initialize web server
    val service = system.actorOf(Props[WebServer], "web-server")
    IO(Http) ? Http.Bind(service, interface="0.0.0.0", port=config.port) onComplete {
      case Success(Http.Bound(endpoint)) => Console.println("Listening")
      case _ => {
        Console.println(s"Failed to bind to 0.0.0.0:${config.port}")
        system.shutdown()
        sys.exit(1)
      }
    }

    while(true) {
      val pipelines = Await.result[List[Pipeline]]((api ? PipelineQuery).mapTo[List[Pipeline]], 5.seconds)
      pipelines.foreach { pipeline =>
        (pipeline.state, pipelineMonitors.get(pipeline.pipeline_id)) match {
          case (PipelineState.RUNNING, None) => {
            PipelineMonitor.of(api, pipeline, sparkContext, sqlContext, sparkStreamingContext) match {
              case Some(pipelineMonitor)  => {
                pipelineMonitor.ensureStarted
                pipelineMonitors += (pipeline.pipeline_id -> pipelineMonitor)
              }
              case None => //failed to start pipeline, state is now ERROR
            }
          }
          case (PipelineState.RUNNING, Some(pipelineMonitor)) => {
            pipelineMonitor.ensureStarted
          }
          case (PipelineState.STOPPED, Some(pipelineMonitor)) => {
            pipelineMonitor.stop
            pipelineMonitors = pipelineMonitors.filterKeys(_ != pipeline.pipeline_id)
          }
          case (_, _) => //do nothing
        }
      }

      // remove pipelines which have been deleted via the API
      val currentPipelineIds = pipelines.map(_.pipeline_id).toSet
      pipelineMonitors.foreach((p: (String, PipelineMonitor)) => {
        val pipeline_id = p._1
        if (!currentPipelineIds.contains(pipeline_id)) {
          val pipelineMonitor = p._2
          pipelineMonitor.stop
        }
      })
      pipelineMonitors = pipelineMonitors -- currentPipelineIds

      Thread.sleep(5000)
    }
  }
}
