package com.memsql.superapp

import akka.pattern.ask
import akka.actor.ActorRef
import com.memsql.spark.context.{MemSQLSQLContext, MemSQLSparkContext}
import com.memsql.spark.etl.api.{ETLPipeline, KafkaExtractor, MemSQLLoader}
import com.memsql.spark.etl.api.configs._
import com.memsql.superapp.api.{ApiActor, PipelineState, Pipeline}
import ApiActor._
import com.memsql.superapp.util.{BaseException, JarLoader}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Time, StreamingContext}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}
import spray.json._

case class PipelineConfigException(message: String) extends BaseException(message: String)

object PipelineMonitor {
  def of(api: ActorRef,
         pipeline: Pipeline,
         sparkContext: MemSQLSparkContext,
         sqlContext: MemSQLSQLContext,
         streamingContext: StreamingContext): Option[PipelineMonitor] = {
    try {
      val clazz = JarLoader.loadClass(pipeline.jar, pipeline.main_class)
      //TODO does this pollute the classpath for the lifetime of the superapp?
      //TODO if an updated jar is appended to the classpath the superapp will always run the old version
      //distribute jar to all tasks run by this spark context
      sparkContext.addJar(pipeline.jar)
      val pipelineInstance = clazz.newInstance.asInstanceOf[ETLPipeline[Any]]
      Some(PipelineMonitor(api, pipeline.pipeline_id, pipeline.config, pipelineInstance, streamingContext, sqlContext))
    } catch {
      case e: Exception => {
        val errorMessage = Some(s"Failed to load class for pipeline ${pipeline.pipeline_id}: $e")
        Console.println(errorMessage)
        e.printStackTrace
        val future = (api ? PipelineUpdate(pipeline.pipeline_id, PipelineState.ERROR, error = errorMessage)).mapTo[Try[Boolean]]
        future.map {
          case Success(resp) => //exit
          case Failure(error) => Console.println(s"Failed to update pipeline ${pipeline.pipeline_id} state to ERROR: $error")
        }
        None
      }
    }
  }
}

case class PipelineMonitor(api: ActorRef,
                           pipeline_id: String,
                           pipelineConfig: PipelineConfig,
                           pipelineInstance: ETLPipeline[Any],
                           streamingContext: StreamingContext,
                           sqlContext: MemSQLSQLContext) {
  private var exception: Exception = null
  val BATCH_DURATION = 5000

  private val thread = new Thread(new Runnable {
    override def run(): Unit = {
      try {
        Console.println(s"Starting pipeline $pipeline_id")
        val future = (api ? PipelineUpdate(pipeline_id, PipelineState.RUNNING)).mapTo[Try[Boolean]]
        future.map {
          case Success(resp) => runPipeline
          case Failure(error) => Console.println(s"Failed to update pipeline $pipeline_id state to RUNNING: $error")
        }
      } catch {
        case e: InterruptedException => //exit
        case e: Exception => {
          exception = e
          Console.println(s"Unexpected exception: $e")
          val future = (api ? PipelineUpdate(pipeline_id, PipelineState.ERROR, error = Some(e.toString))).mapTo[Try[Boolean]]
          future.map {
            case Success(resp) => //exit
            case Failure(error) => Console.println(s"Failed to update pipeline $pipeline_id state to ERROR: $error")
          }
        }
      }
    }
  })

  def runPipeline(): Unit = {
    val extractor = pipelineConfig.extract.get.kind match {
      case ExtractPhaseKind.Kafka => new KafkaExtractor
      case ExtractPhaseKind.User => pipelineInstance.extractor
    }
    val transformer = pipelineConfig.transform.get.kind match {
      case TransformPhaseKind.User => pipelineInstance.transformer
    }
    val loader = pipelineConfig.load.get.kind match {
      case LoadPhaseKind.MemSQL => new MemSQLLoader
      case LoadPhaseKind.User => pipelineInstance.loader
    }

    var extractConfig: PhaseConfig = null
    var transformConfig: PhaseConfig = null
    var loadConfig: PhaseConfig = null
    try {
      extractConfig = ExtractPhase.readConfig(pipelineConfig.extract.get.kind, pipelineConfig.extract.get.config)
      transformConfig = TransformPhase.readConfig(pipelineConfig.transform.get.kind, pipelineConfig.transform.get.config)
      loadConfig = LoadPhase.readConfig(pipelineConfig.load.get.kind, pipelineConfig.load.get.config)
    } catch {
      case e: DeserializationException => throw new PipelineConfigException(s"config does not validate: $e")
    }

    val inputDStream = extractor.extract(streamingContext, extractConfig)
    var time: Long = 0

    // manually compute the next RDD in the DStream so that we can sidestep issues with
    // adding inputs to the streaming context at runtime
    while (true) {
      time = System.currentTimeMillis

      inputDStream.compute(Time(time)) match {
        case Some(rdd) => {
          val df = transformer.transform(sqlContext, rdd.asInstanceOf[RDD[Any]], transformConfig)
          loader.load(df, loadConfig)

          Console.println(s"${inputDStream.count()} rows after extract")
          Console.println(s"${df.count()} rows after transform")
        }
        case None =>
      }

      Thread.sleep(Math.max(BATCH_DURATION - (System.currentTimeMillis - time), 0))
    }
  }

  def start(): Unit = {
    thread.start
  }

  def isAlive(): Boolean = {
    thread.isAlive
  }

  def stop() = {
    thread.interrupt
    thread.join
  }
}
