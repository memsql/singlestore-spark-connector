package com.memsql.superapp

import akka.pattern.ask
import akka.actor.ActorRef
import com.memsql.spark.context.{MemSQLSparkContext, MemSQLSQLContext}
import com.memsql.spark.etl.api._
import com.memsql.spark.etl.api.{KafkaExtractor, MemSQLLoader}
import com.memsql.spark.etl.api.configs._
import com.memsql.spark.etl.utils.Logging
import com.memsql.superapp.api.{PipelineInstance, ApiActor, PipelineState, Pipeline}
import java.util.concurrent.atomic.AtomicBoolean
import ApiActor._
import com.memsql.superapp.util.JarLoader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Time, StreamingContext}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait PipelineMonitor {
  def api: ActorRef
  def pipeline_id: String
  def batchInterval: Long
  def lastUpdated: Long
  def jar: String
  def config: PipelineConfig
  def pipelineInstance: PipelineInstance[Any]
  def sparkContext: SparkContext
  def streamingContext: StreamingContext
  def sqlContext: SQLContext

  def runPipeline: Unit
  def ensureStarted: Unit
  def isAlive: Boolean
  def stop: Unit
}
class DefaultPipelineMonitor(override val api: ActorRef,
                                      val pipeline: Pipeline,
                             override val sparkContext: SparkContext,
                             override val streamingContext: StreamingContext) extends PipelineMonitor with Logging {

  override val pipeline_id = pipeline.pipeline_id

  // keep a copy of the pipeline info so we can determine when the pipeline has been updated
  override val batchInterval = pipeline.batch_interval
  override val config = pipeline.config
  override val lastUpdated = pipeline.last_updated
  override val jar = pipeline.jar

  private[superapp] var loadJar = false

  private[superapp] val extractConfig = ExtractPhase.readConfig(pipeline.config.extract.kind, pipeline.config.extract.config)
  private[superapp] val transformConfig = TransformPhase.readConfig(pipeline.config.transform.kind, pipeline.config.transform.config)
  private[superapp] val loadConfig = LoadPhase.readConfig(pipeline.config.load.kind, pipeline.config.load.config)

  private[superapp] val extractor: Extractor[Any] = pipeline.config.extract.kind match {
    case ExtractPhaseKind.Kafka => new KafkaExtractor(pipeline.pipeline_id).asInstanceOf[Extractor[Any]]
    case ExtractPhaseKind.TestString | ExtractPhaseKind.TestJson => new ConfigStringExtractor().asInstanceOf[Extractor[Any]]
    case ExtractPhaseKind.User => {
      loadJar = true
      val className = extractConfig.asInstanceOf[UserExtractConfig].class_name
      JarLoader.loadClass(pipeline.jar, className).newInstance.asInstanceOf[Extractor[Any]]
    }
  }
  private[superapp] val transformer: Transformer[Any] = pipeline.config.transform.kind match {
    case TransformPhaseKind.Json => {
      //TODO instead of matching on the extract kind, we should match on the type of the produced RDD
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
  private[superapp] val loader: Loader = pipeline.config.load.kind match {
    case LoadPhaseKind.MemSQL => new MemSQLLoader
    case LoadPhaseKind.User => {
      loadJar = true
      val className = loadConfig.asInstanceOf[UserLoadConfig].class_name
      JarLoader.loadClass(pipeline.jar, className).newInstance.asInstanceOf[Loader]
    }
  }

  override val pipelineInstance = PipelineInstance(extractor, extractConfig, transformer, transformConfig, loader, loadConfig)

  if (loadJar) {
    //TODO does this pollute the classpath for the lifetime of the superapp?
    //TODO if an updated jar is appended to the classpath the superapp will always run the old version
    //distribute jar to all tasks run by this spark context
    sparkContext.addJar(pipeline.jar)
  }

  override val sqlContext = sparkContext.isInstanceOf[MemSQLSparkContext] match {
    case true => new MemSQLSQLContext(sparkContext.asInstanceOf[MemSQLSparkContext])
    case false => new SQLContext(sparkContext)
  }

  private[superapp] val isStopping = new AtomicBoolean()

  private[superapp] val thread = new Thread(new Runnable {
    override def run(): Unit = {
      try {
        logInfo(s"Starting pipeline $pipeline_id")
        val future = (api ? PipelineUpdate(pipeline_id, PipelineState.RUNNING)).mapTo[Try[Boolean]]
        future.map {
          case Success(resp) => runPipeline
          case Failure(error) => logError(s"Failed to update pipeline $pipeline_id state to RUNNING", error)
        }
      } catch {
        case e: InterruptedException => //exit
        case e: Exception => {
          logError(s"Unexpected exception: $e")
          val future = (api ? PipelineUpdate(pipeline_id, PipelineState.ERROR, error = Some(e.toString))).mapTo[Try[Boolean]]
          future.map {
            case Success(resp) => //exit
            case Failure(error) => logError(s"Failed to update pipeline $pipeline_id state to ERROR", error)
          }
        }
      }
    }
  })

  def runPipeline(): Unit = {
    var exception: Option[Throwable] = None
    var inputDStream: InputDStream[Any] = null

    try {
      logDebug("Initializing extractor")
      inputDStream = pipelineInstance.extractor.extract(streamingContext, pipelineInstance.extractConfig, batchInterval)
      logDebug("Starting InputDStream")
      inputDStream.start()

      var time: Long = 0

      // manually compute the next RDD in the DStream so that we can sidestep issues with
      // adding inputs to the streaming context at runtime
      while (!isStopping.get) {
        time = System.currentTimeMillis

        logDebug(s"Computing next RDD from InputDStream: $time")
        inputDStream.compute(Time(time)) match {
          case Some(rdd) => {
            logDebug("Transforming RDD")
            val df = pipelineInstance.transformer.transform(sqlContext, rdd.asInstanceOf[RDD[Any]], pipelineInstance.transformConfig)
            logDebug("Loading RDD")
            pipelineInstance.loader.load(df, pipelineInstance.loadConfig)
          }
          case None => logDebug("No RDD from InputDStream")
        }

        val sleepTimeMillis = Math.max(batchInterval - (System.currentTimeMillis - time), 0)
        logDebug(s"Sleeping for $sleepTimeMillis milliseconds")
        Thread.sleep(sleepTimeMillis)
      }
    } catch {
      case NonFatal(e) => {
        logError(s"Unexpected error in pipeline $pipeline_id", e)
        exception = Some(e)
      }
    } finally {
      logInfo(s"Stopping pipeline $pipeline_id")
      try {
        if (inputDStream != null) inputDStream.stop()
      } catch {
        case NonFatal(e) => {
          logError(s"Exception in pipeline $pipeline_id while stopping extractor", e)
        }
      }
    }
  }

  override def ensureStarted() = {
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
    logDebug(s"Stopping pipeline thread $pipeline_id")
    isStopping.set(true)
    thread.interrupt
    thread.join
  }
}
