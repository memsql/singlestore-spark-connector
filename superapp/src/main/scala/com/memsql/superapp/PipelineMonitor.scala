package com.memsql.superapp

import akka.pattern.ask
import akka.actor.ActorRef
import com.memsql.spark.context.{MemSQLSQLContext, MemSQLSparkContext}
import com.memsql.spark.etl.api._
import com.memsql.spark.etl.api.configs._
import com.memsql.superapp.api.{PipelineInstance, ApiActor, PipelineState, Pipeline}
import java.util.concurrent.atomic.AtomicBoolean
import ApiActor._
import com.memsql.superapp.util.{BaseException, JarLoader}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Time, StreamingContext}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal
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
      var loadJar = false

      val extractConfig = ExtractPhase.readConfig(pipeline.config.extract.kind, pipeline.config.extract.config)
      val transformConfig = TransformPhase.readConfig(pipeline.config.transform.kind, pipeline.config.transform.config)
      val loadConfig = LoadPhase.readConfig(pipeline.config.load.kind, pipeline.config.load.config)

      val extractor: Extractor[Any] = pipeline.config.extract.kind match {
        case ExtractPhaseKind.Kafka => new KafkaExtractor(pipeline.pipeline_id).asInstanceOf[Extractor[Any]]
        case ExtractPhaseKind.User => {
          loadJar = true
          val className = extractConfig.asInstanceOf[UserExtractConfig].class_name
          JarLoader.loadClass(pipeline.jar, className).newInstance.asInstanceOf[Extractor[Any]]
        }
      }
      val transformer: Transformer[Any] = pipeline.config.transform.kind match {
        case TransformPhaseKind.Json => {
          pipeline.config.extract.kind match {
            case ExtractPhaseKind.Kafka => JSONTransformer.makeSimpleJSONKeyValueTransformer("json").asInstanceOf[Transformer[Any]]
            case default => JSONTransformer.makeSimpleJSONTransformer("json")
          }
        }
        case TransformPhaseKind.User => {
          loadJar = true
          val className = transformConfig.asInstanceOf[UserTransformConfig].class_name
          JarLoader.loadClass(pipeline.jar, className).newInstance.asInstanceOf[Transformer[Any]]
        }
      }
      val loader: Loader = pipeline.config.load.kind match {
        case LoadPhaseKind.MemSQL => new MemSQLLoader
        case LoadPhaseKind.User => {
          loadJar = true
          val className = loadConfig.asInstanceOf[UserLoadConfig].class_name
          JarLoader.loadClass(pipeline.jar, className).newInstance.asInstanceOf[Loader]
        }
      }

      val pipelineInstance = PipelineInstance(extractor, extractConfig,
                                              transformer, transformConfig,
                                              loader, loadConfig)

      if (loadJar) {
        //TODO does this pollute the classpath for the lifetime of the superapp?
        //TODO if an updated jar is appended to the classpath the superapp will always run the old version
        //distribute jar to all tasks run by this spark context
        sparkContext.addJar(pipeline.jar)
      }

      Some(PipelineMonitor(api, pipeline.pipeline_id, pipeline.batch_interval, pipeline.config, pipelineInstance, streamingContext, sqlContext))
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
                           batch_interval: Long,
                           pipelineConfig: PipelineConfig,
                           pipelineInstance: PipelineInstance[Any],
                           streamingContext: StreamingContext,
                           sqlContext: MemSQLSQLContext) {
  private var exception: Exception = null
  val isStopping = new AtomicBoolean()

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
    val inputDStream = pipelineInstance.extractor.extract(streamingContext, pipelineInstance.extractConfig)
    inputDStream.start()

    try {
      var time: Long = 0

      // manually compute the next RDD in the DStream so that we can sidestep issues with
      // adding inputs to the streaming context at runtime
      while (!isStopping.get) {
        time = System.currentTimeMillis

        inputDStream.compute(Time(time)) match {
          case Some(rdd) => {
            val df = pipelineInstance.transformer.transform(sqlContext, rdd.asInstanceOf[RDD[Any]], pipelineInstance.transformConfig)
            pipelineInstance.loader.load(df, pipelineInstance.loadConfig)

            Console.println(s"${inputDStream.count()} rows after extract")
            Console.println(s"${df.count()} rows after transform")
          }
          case None =>
        }

        Thread.sleep(Math.max(batch_interval - (System.currentTimeMillis - time), 0))
      }
    } finally {
      inputDStream.stop()
    }
  }

  def ensureStarted() = {
    try {
      thread.start
    } catch {
      case e: IllegalThreadStateException => {}
    }
  }

  def isAlive(): Boolean = {
    thread.isAlive
  }

  def stop() = {
    isStopping.set(true)
    thread.interrupt
    thread.join
  }
}
