package com.memsql.spark.interface

import akka.pattern.ask
import akka.actor.ActorRef
import com.memsql.spark.context.{MemSQLSparkContext, MemSQLSQLContext}
import com.memsql.spark.etl.api._
import com.memsql.spark.etl.api.{KafkaExtractor, MemSQLLoader}
import com.memsql.spark.etl.api.configs._
import com.memsql.spark.etl.utils.Logging
import com.memsql.spark.interface.api.{PipelineInstance, Pipeline, PipelineState, ApiActor, PipelineMetricRecord, PhaseMetricRecord}
import java.util.concurrent.atomic.AtomicBoolean
import ApiActor._
import com.memsql.spark.interface.util.JarLoader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
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
  override val jar = config.jar.orNull

  private[interface] var jarLoaded = false
  private[interface] var jarClassLoader: ClassLoader = null

  private def loadClass(path: String, clazz: String): Class[_] = {
    if (!jarLoaded) {
      jarClassLoader = JarLoader.getClassLoader(path)
      jarLoaded = true
    }

    JarLoader.loadClass(jarClassLoader, clazz)
  }

  private[interface] val extractConfig = ExtractPhase.readConfig(config.extract.kind, config.extract.config)
  private[interface] val transformConfig = TransformPhase.readConfig(config.transform.kind, config.transform.config)
  private[interface] val loadConfig = LoadPhase.readConfig(config.load.kind, config.load.config)

  private[interface] val extractor: Extractor[Any] = config.extract.kind match {
    case ExtractPhaseKind.Kafka => new KafkaExtractor(pipeline_id).asInstanceOf[Extractor[Any]]
    case ExtractPhaseKind.TestString | ExtractPhaseKind.TestJson => new ConfigStringExtractor().asInstanceOf[Extractor[Any]]
    case ExtractPhaseKind.User => {
      val className = extractConfig.asInstanceOf[UserExtractConfig].class_name
      loadClass(jar, className).newInstance.asInstanceOf[Extractor[Any]]
    }
  }
  private[interface] val transformer: Transformer[Any] = config.transform.kind match {
    case TransformPhaseKind.Json => {
      //TODO instead of matching on the extract kind, we should match on the type of the produced RDD
      config.extract.kind match {
        case ExtractPhaseKind.Kafka => JSONTransformer.makeSimpleJSONKeyValueTransformer("json").asInstanceOf[Transformer[Any]]
        case default => JSONTransformer.makeSimpleJSONTransformer("json")
      }
    }
    case TransformPhaseKind.User => {
      val className = transformConfig.asInstanceOf[UserTransformConfig].class_name
      loadClass(jar, className).newInstance.asInstanceOf[Transformer[Any]]
    }
  }
  private[interface] val loader: Loader = config.load.kind match {
    case LoadPhaseKind.MemSQL => new MemSQLLoader
    case LoadPhaseKind.User => {
      val className = loadConfig.asInstanceOf[UserLoadConfig].class_name
      loadClass(jar, className).newInstance.asInstanceOf[Loader]
    }
  }

  override val pipelineInstance = PipelineInstance(extractor, extractConfig, transformer, transformConfig, loader, loadConfig)

  if (jarLoaded) {
    //TODO does this pollute the classpath for the lifetime of the interface?
    //TODO if an updated jar is appended to the classpath the interface will always run the old version
    //distribute jar to all tasks run by this spark context
    sparkContext.addJar(jar)
  }

  override val sqlContext = sparkContext.isInstanceOf[MemSQLSparkContext] match {
    case true => new MemSQLSQLContext(sparkContext.asInstanceOf[MemSQLSparkContext])
    case false => new SQLContext(sparkContext)
  }

  private[interface] val isStopping = new AtomicBoolean()

  private[interface] val thread = new Thread(new Runnable {
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
          logError(s"Unexpected exception for pipeline $pipeline_id", e)
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
      logDebug(s"Initializing extractor for pipeline $pipeline_id")
      inputDStream = pipelineInstance.extractor.extract(streamingContext, pipelineInstance.extractConfig, batchInterval)
      logDebug(s"Starting InputDStream for pipeline $pipeline_id")
      inputDStream.start()

      var time: Long = 0

      // manually compute the next RDD in the DStream so that we can sidestep issues with
      // adding inputs to the streaming context at runtime
      while (!isStopping.get) {
        time = System.currentTimeMillis

        var success = false
        var extractRecord: Option[PhaseMetricRecord] = None
        var transformRecord: Option[PhaseMetricRecord] = None
        var loadRecord: Option[PhaseMetricRecord] = None

        logDebug(s"Computing next RDD for pipeline $pipeline_id: $time")

        var extractedRdd: RDD[Any] = null
        extractRecord = runPhase(() => {
          inputDStream.compute(Time(time)) match {
            case Some(rdd) => extractedRdd = rdd
            case None => logDebug(s"No RDD from pipeline $pipeline_id")
          }
        })

        var df: DataFrame = null
        if (extractedRdd != null) {
          transformRecord = runPhase(() => {
            logDebug(s"Transforming RDD for pipeline $pipeline_id")
            df = pipelineInstance.transformer.transform(
              sqlContext,
              extractedRdd.asInstanceOf[RDD[Any]],
              pipelineInstance.transformConfig)
          })
        }

        if (df != null) {
          loadRecord = runPhase(() => {
            logDebug(s"Loading RDD for pipeline $pipeline_id")
            pipelineInstance.loader.load(df, pipelineInstance.loadConfig)
            success = true
          })
        }

        val metric = PipelineMetricRecord(
          pipeline_id = pipeline_id,
          timestamp = time,
          success = success,
          extract = extractRecord,
          transform = transformRecord,
          load = loadRecord)
        pipeline.enqueueMetricRecord(metric)

        val sleepTimeMillis = Math.max(batchInterval - (System.currentTimeMillis - time), 0)
        logDebug(s"Sleeping for $sleepTimeMillis milliseconds for pipeline $pipeline_id")
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

  def runPhase(fn: () => Unit): Option[PhaseMetricRecord] = {
    var error: Option[String] = None
    val startTime = System.currentTimeMillis
    try {
      fn()
    } catch {
      case NonFatal(e) => {
        logError(s"Phase error in pipeline $pipeline_id", e)
        error = Some(e.toString)
      }
    }
    val stopTime = System.currentTimeMillis
    Some(PhaseMetricRecord(
      start = startTime,
      stop = stopTime,
      count = None,
      error = error,
      records = None,
      logs = None
    ))
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
