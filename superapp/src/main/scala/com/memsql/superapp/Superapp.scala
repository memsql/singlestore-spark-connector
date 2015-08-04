package com.memsql.superapp

import com.memsql.superapp.server.WebServer

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import com.memsql.superapp.util.{JarLoader, Paths}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import com.memsql.spark.context.{MemSQLSparkContext, MemSQLSQLContext}
import spray.can.Http
import akka.pattern.ask
import scala.concurrent._
import scala.concurrent.duration._
import akka.util.Timeout
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
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("superapp") {
      override def showUsageOnError = true

      head("superapp", "0.1.2")
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
    var pipelineMonitors = Map[String, Runnable]()

    // initialize api
    val api = system.actorOf(Props(classOf[ApiActor], config), "api")

    // initialize web server
    val service = system.actorOf(Props[WebServer], "web-server")
    implicit val timeout = Timeout(5.seconds)
    IO(Http) ? Http.Bind(service, interface="0.0.0.0", port=config.port) onComplete {
      case Success(Http.Bound(endpoint)) => Console.println("Listening")
      case _ => {
        Console.println(s"Failed to bind to 0.0.0.0:${config.port}")
        system.shutdown()
        sys.exit(1)
      }
    }

    while(true) {
      Await.ready((api ? PipelineQuery).mapTo[List[Pipeline]], 5.seconds).map { pipelines =>
        pipelines.foreach { pipeline =>
          var nextState = pipeline.state
          var error = ""
          (pipeline.state, pipelineMonitors.get(pipeline.pipeline_id)) match {
            case (PipelineState.RUNNING, None) => {
              try {
                val clazz = JarLoader.loadClass(pipeline.jar, pipeline.main_class)
                //TODO does this pollute the classpath for the lifetime of the superapp?
                //TODO if an updated jar is appended to the classpath the superapp will always run the old version
                //distribute jar to all tasks run by this spark context
                sparkContext.addJar(pipeline.jar)
                val pipelineInstance = clazz.newInstance.asInstanceOf[{def run(sc: StreamingContext, sqlContext: SQLContext)}]
                val pipelineThread = new Thread {
                  override def run {
                    Console.println(s"Starting pipeline ${pipeline.pipeline_id}")
                    nextState = PipelineState.RUNNING
                    pipelineInstance.run(sparkStreamingContext, sqlContext)
                  }
                }
                pipelineThread.start
                pipelineMonitors = pipelineMonitors + (pipeline.pipeline_id -> pipelineThread)
              } catch {
                case e: Exception =>
                  error = s"Failed to load class for pipeline: $e"
                  nextState = PipelineState.ERROR
                  Console.println(s"Failed to load class for pipeline: $e")
                  e.printStackTrace()
              }
            }
            case (PipelineState.RUNNING, Some(pipelineMonitor)) => //TODO monitor running pipeline and set state to stopped or error
            case (PipelineState.STOPPED, Some(pipelineMonitor)) => //TODO stop the running pipeline
            case (PipelineState.ERROR, Some(pipelineMonitor)) => //TODO record error
            case (_, _) => //do nothing
          }

          val future = (api ? PipelineUpdate(pipeline.pipeline_id, nextState, error = error)).mapTo[Try[Boolean]]
          future.map {
            case Success(resp) => //ignore
            case Failure(error) => Console.println(s"Failed to update pipeline ${pipeline.pipeline_id} to state $nextState, $error")
          }
        }
      }

      Thread.sleep(5000)
    }
  }
}
