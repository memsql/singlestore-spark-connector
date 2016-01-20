package com.memsql.spark.interface

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.UUID
import akka.pattern.ask
import akka.actor.ActorRef
import com.memsql.spark.SaveToMemSQLException
import com.memsql.spark.connector.MemSQLContext
import com.memsql.spark.interface.api.PipelineThreadState.PipelineThreadState
import com.memsql.spark.phases._
import com.memsql.spark.etl.api._
import com.memsql.spark.etl.api.configs._
import com.memsql.spark.phases.configs._
import com.memsql.spark.etl.utils.Logging
import com.memsql.spark.interface.api._
import com.memsql.spark.interface.api.PipelineBatchType.PipelineBatchType
import com.memsql.spark.interface.util.ErrorUtils._
import ApiActor._
import com.memsql.spark.interface.util.{PipelineLogger, BaseException}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.ui.jobs.JobProgressListener
import scala.collection.mutable.HashSet
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.{ControlThrowable, NonFatal}
import scala.util.{Failure, Success, Try}

class PipelineMonitorException(message: String) extends BaseException(message)

case class PhaseResult(count: Option[Long] = None,
                       columns: Option[List[(String, String)]] = None,
                       records: Option[List[List[String]]] = None)

trait PipelineMonitor {
  def api: ActorRef
  def pipeline_id: String // scalastyle:ignore
  def singleStep: Boolean
  def batchInterval: Long
  def lastUpdated: Long
  def config: PipelineConfig
  def traceBatchCount: Int
  def pipelineInstance: PipelineInstance
  def sparkContext: SparkContext
  def streamingContext: StreamingContext
  def sqlContext: SQLContext

  def runPipeline: Unit
  def ensureStarted: Unit
  def isAlive: Boolean
  def hasError: Boolean
  var error: Throwable
  def stop: Unit
}
class DefaultPipelineMonitor(override val api: ActorRef,
                                      val pipeline: Pipeline,
                             override val sparkContext: SparkContext,
                             override val streamingContext: StreamingContext,
                                      val jobProgressListener: JobProgressListener = null) extends PipelineMonitor with Logging {
  override val pipeline_id = pipeline.pipeline_id

  // keep a copy of the pipeline info so we can determine when the pipeline has been updated
  override val singleStep = pipeline.single_step
  override val batchInterval = pipeline.batch_interval
  private val batchIntervalMillis = DurationLong(batchInterval).seconds.toMillis
  override val config = pipeline.config
  override val lastUpdated = pipeline.last_updated
  override var error: Throwable = null
  override def traceBatchCount(): Int = pipeline.traceBatchCount

  val TRACED_RECORDS_PER_BATCH = 10

  private[interface] val extractConfig = ExtractPhase.readConfig(config.extract.kind, config.extract.config)
  private[interface] val transformConfig = TransformPhase.readConfig(config.transform.kind, config.transform.config)
  private[interface] val loadConfig = LoadPhase.readConfig(config.load.kind, config.load.config)

  override val sqlContext = new MemSQLContext(sparkContext)

  private[interface] val extractor: Extractor = config.extract.kind match {
    case ExtractPhaseKind.Kafka => new KafkaExtractor()
    case ExtractPhaseKind.ZookeeperManagedKafka => new ZookeeperManagedKafkaExtractor()
    case ExtractPhaseKind.S3 => new S3Extractor()
    case ExtractPhaseKind.TestLines => new TestLinesExtractor()
    case ExtractPhaseKind.User => {
      val className = extractConfig.asInstanceOf[UserExtractConfig].class_name
      Class.forName(className).newInstance.asInstanceOf[Extractor]
    }
    case ExtractPhaseKind.Python => {
      new PythonExtractor(sparkContext, pipeline_id, extractConfig.asInstanceOf[PythonExtractConfig])
    }
  }
  private[interface] val transformer: Transformer = config.transform.kind match {
    case TransformPhaseKind.Json => new JSONTransformer
    case TransformPhaseKind.Csv => new CSVTransformer
    case TransformPhaseKind.CsvSampling => new CSVSamplingTransformer
    case TransformPhaseKind.Identity => new IdentityTransformer
    case TransformPhaseKind.User => {
      val className = transformConfig.asInstanceOf[UserTransformConfig].class_name
      Class.forName(className).newInstance.asInstanceOf[Transformer]
    }
    case TransformPhaseKind.Python => {
      new PythonTransformer(sparkContext, pipeline_id, transformConfig.asInstanceOf[PythonTransformConfig])
    }
  }
  private[interface] val loader: Loader = config.load.kind match {
    case LoadPhaseKind.MemSQL => new MemSQLLoader
  }

  override val pipelineInstance = PipelineInstance(extractor, extractConfig, transformer, transformConfig, loader, loadConfig)

  private var currentBatchId: String = null
  private var currentBatchType: PipelineBatchType = null
  private[interface] val isStopping = new AtomicBoolean()

  private[interface] val thread = new Thread(new Runnable {
    override def run(): Unit = {
      try {
        logInfo(s"Starting pipeline $pipeline_id")
        val future = (api ? PipelineUpdate(pipeline_id, Some(PipelineState.RUNNING), error = Some(""),
          threadState = Some(PipelineThreadState.THREAD_RUNNING))).mapTo[Try[Boolean]]
        Await.result[Try[Boolean]](future, 5.seconds) match {
          case Success(resp) => runPipeline
          case Failure(error) => logError(s"Failed to update pipeline $pipeline_id state to RUNNING", error)
        }
      } catch {
        case e: InterruptedException => //exit
        case e: TimeoutException => {
          throw new PipelineMonitorException(s"Timed out updating pipeline $pipeline_id state to RUNNING")
        }
      }
    }
  })

  def runPipeline(): Unit = {
    var extractorInitialized: Boolean = false
    val extractLogger = getPhaseLogger("extract")

    var transformerInitialized: Boolean = false
    val transformLogger = getPhaseLogger("transform", trackEntries=false)

    try {
      logDebug(s"Initializing extractor for pipeline $pipeline_id")
      pipelineInstance.extractor.initialize(streamingContext, sqlContext, pipelineInstance.extractConfig, batchIntervalMillis, extractLogger)
      extractorInitialized = true

      logDebug(s"Initializing transformer for pipeline $pipeline_id")
      pipelineInstance.transformer.initialize(sqlContext, pipelineInstance.transformConfig, transformLogger)
      transformerInitialized = true

      var time: Long = 0
      val pipelineStartEvent = PipelineStartEvent(
        pipeline_id = pipeline_id,
        timestamp = System.currentTimeMillis,
        event_id = UUID.randomUUID.toString)
      pipeline.enqueueMetricRecord(pipelineStartEvent)

     if (singleStep) {
       stepPipeline(extractLogger, transformLogger, System.currentTimeMillis)

       (api ? PipelineUpdate(pipeline_id,
         state = Some(PipelineState.FINISHED))).mapTo[Try[Boolean]]
     } else {
        while (!isStopping.get) {
          time = System.currentTimeMillis

          stepPipeline(extractLogger, transformLogger, time)

          val sleepTimeMillis = Math.max(batchIntervalMillis - (System.currentTimeMillis - time), 0)
          logDebug(s"Sleeping for $sleepTimeMillis milliseconds for pipeline $pipeline_id")
          Thread.sleep(sleepTimeMillis)
        }
     }
    } catch {
      case e: ControlThrowable => {
        enqueueCancelEvent()
        throw e
      }
      case e: InterruptedException => {
        enqueueCancelEvent()
        throw e
      }
      case e: Throwable => {
        logError(s"Exception in pipeline $pipeline_id", e)
        enqueueCancelEvent()
        error = e
      }
    } finally {
      (api ? PipelineUpdate(pipeline_id,
        threadState = Some(PipelineThreadState.THREAD_STOPPED))).mapTo[Try[Boolean]]
      logInfo(s"Stopping pipeline $pipeline_id")
      val pipelineEndEvent = PipelineEndEvent(
        pipeline_id = pipeline_id,
        timestamp = System.currentTimeMillis,
        event_id = UUID.randomUUID.toString)
      pipeline.enqueueMetricRecord(pipelineEndEvent)

      try {
        if (extractorInitialized) {
          pipelineInstance.extractor.cleanup(streamingContext, sqlContext, pipelineInstance.extractConfig, batchInterval, extractLogger)
        }
      } catch {
        case NonFatal(e) => {
          logError(s"Exception in pipeline $pipeline_id while stopping extractor", e)
        }
      }

      try {
        if (transformerInitialized) {
          pipelineInstance.transformer.cleanup(sqlContext, pipelineInstance.transformConfig, transformLogger)
        }
      } catch {
        case NonFatal(e) => {
          logError(s"Exception in pipeline $pipeline_id while stopping transformer", e)
        }
      }
    }
  }

  // manually compute the next DataFrame so that we can sidestep issues with
  // adding inputs to the streaming context at runtime
  def stepPipeline(extractLogger: PipelineLogger, transformLogger: PipelineLogger, time: Long): Unit = {
    var trace = false
    if (pipeline.traceBatchCount > 0) {
      trace = true
      val future = (api ? PipelineTraceBatchDecrement(pipeline_id)).mapTo[Try[Boolean]]
      future.map {
        case Success(resp) =>
        case Failure(error) => logError(s"Failed to decrement pipeline $pipeline_id trace batch count", error)
      }
    }

    var extractRecord: Option[PhaseMetricRecord] = None
    var transformRecord: Option[PhaseMetricRecord] = None
    var loadRecord: Option[PhaseMetricRecord] = None

    logDebug(s"Computing next DataFrame for pipeline $pipeline_id: $time")

    val batch_id = UUID.randomUUID.toString
    currentBatchId = batch_id
    var batch_type = PipelineBatchType.Normal
    if (trace) {
      batch_type = PipelineBatchType.Traced
    }
    currentBatchType = batch_type

    val batchStartEvent = BatchStartEvent(
      batch_id = batch_id,
      batch_type = batch_type,
      pipeline_id = pipeline_id,
      timestamp = System.currentTimeMillis,
      event_id = UUID.randomUUID.toString)
    pipeline.enqueueMetricRecord(batchStartEvent)
    sparkContext.setJobGroup(batch_id, s"Batch for MemSQL Pipeline $pipeline_id", interruptOnCancel = true)
    var extractedTraceDf: DataFrame = null
    var extractedDf: DataFrame = null
    var extractedDfIsEmpty: Boolean = false
    var upperBound: Long = 0
    extractRecord = runPhase(extractLogger, trace, () => {
      val maybeDf = pipelineInstance.extractor.next(streamingContext, time, sqlContext, pipelineInstance.extractConfig,
        batchInterval, extractLogger)
      maybeDf match {
        case null => throw new PipelineMonitorException(s"Extractor for pipeline $pipeline_id emitted null instead of None or Some(RDD)")
        case Some(null) => throw new PipelineMonitorException(s"Extractor for pipeline $pipeline_id emitted Some(null) instead of None or Some(RDD)")
        case Some(df) => {
          if (trace) {
            val count = Some(df.count)
            val weight = math.min(TRACED_RECORDS_PER_BATCH.toFloat / count.get, 1.0)
            val dfs = df.randomSplit(Array(weight, 1.0 - weight))
            extractedTraceDf = dfs(0)
            extractedDf = dfs(1)
            val (columns, records) = getExtractRecords(extractedTraceDf)
            val extractedTraceDfCount = extractedTraceDf.count
            upperBound = math.max(TRACED_RECORDS_PER_BATCH, extractedTraceDfCount)
            if (extractedTraceDfCount == count.get) {
              extractedDfIsEmpty = true
            }
            PhaseResult(count = count, columns = columns, records = records)
          } else {
            extractedDf = df
            PhaseResult()
          }
        }
        case None => {
          logDebug(s"No RDD from pipeline $pipeline_id")
          PhaseResult()
        }
      }
    })

    var transformedTraceDf: DataFrame = null
    var transformedDf: DataFrame = null
    var tracedTransformLogger: PipelineLogger = null
    if (extractedTraceDf != null) {
      // We use a new logger for the trace because we want to
      // only get logs for the traced records.
      tracedTransformLogger = getPhaseLogger("transform")
    }
    var tracedCount: Long = 0
    if (extractedDf != null) {
      transformRecord = runPhase(tracedTransformLogger, trace, () => {
        logDebug(s"Transforming RDD for pipeline $pipeline_id")
        if (extractedTraceDf != null) {
          // We use a new logger for the trace because we want to
          // only get logs for the traced records.
          transformedTraceDf = pipelineInstance.transformer.transform(
            sqlContext, extractedTraceDf, pipelineInstance.transformConfig,
            tracedTransformLogger)
          if (transformedTraceDf == null) {
            throw new PipelineMonitorException(s"Transformer for pipeline $pipeline_id returned null instead of a DataFrame")
          }
          tracedCount = transformedTraceDf.count
        }
        if (extractedDfIsEmpty && transformedTraceDf != null) {
          // If we're in trace mode and all of our records are in
          // extractedTraceDf, don't call transform() with an empty
          // DataFrame, since this can crash Python transformers; instead,
          // just create an empty DataFrame.
          transformedDf = sqlContext.createDataFrame(sparkContext.emptyRDD[Row], transformedTraceDf.schema)
        } else {
          transformedDf = pipelineInstance.transformer.transform(
            sqlContext, extractedDf, pipelineInstance.transformConfig,
            transformLogger)
        }
        if (transformedDf == null) {
          throw new PipelineMonitorException(s"Transformer for pipeline $pipeline_id returned null instead of a DataFrame")
        } else if (trace) {
          var count: Option[Long] = None
          var columns: Option[List[(String, String)]] = None
          var records: Option[List[List[String]]] = None
          if (transformedTraceDf != null) {
            count = Some(tracedCount + transformedDf.count)
            val columnsAndRecords = getTransformRecords(transformedTraceDf, upperBound)
            columns = columnsAndRecords._1
            records = columnsAndRecords._2
          }
          PhaseResult(count = count, columns = columns, records = records)
        } else {
          PhaseResult()
        }
      })
    }

    val loadLogger = getPhaseLogger("load")
    if (transformedDf != null) {
      if (transformedTraceDf != null) {
        transformedDf = transformedDf.unionAll(transformedTraceDf)
      }
      loadRecord = runPhase(loadLogger, trace, () => {
        logDebug(s"Loading RDD for pipeline $pipeline_id")
        val count = Some(pipelineInstance.loader.load(transformedDf, pipelineInstance.loadConfig, loadLogger))
        var columns: Option[List[(String, String)]] = None
        if (trace) {
          columns = getLoadColumns()
        }
        PhaseResult(count = count, columns = columns)
      })
    }

    val task_errors = getTaskErrors(batch_id)

    val success = (
      (extractRecord.isEmpty || extractRecord.get.error.isEmpty) &&
        (transformRecord.isEmpty || transformRecord.get.error.isEmpty) &&
        (loadRecord.isEmpty || loadRecord.get.error.isEmpty) &&
        task_errors.isEmpty
      )

    val batchEndEvent = BatchEndEvent(
      batch_id = batch_id,
      batch_type = batch_type,
      pipeline_id = pipeline_id,
      timestamp = System.currentTimeMillis,
      success = success,
      task_errors = task_errors,
      extract = extractRecord,
      transform = transformRecord,
      load = loadRecord,
      event_id = UUID.randomUUID.toString)
    pipeline.enqueueMetricRecord(batchEndEvent)

  }

  def runPhase(logger: PipelineLogger, trace: Boolean, fn: () => PhaseResult): Option[PhaseMetricRecord] = {
    var error: Option[String] = None
    var count: Option[Long] = None
    var logs: Option[List[String]] = None
    var phaseResult = PhaseResult()
    val startTime = System.currentTimeMillis
    try {
      phaseResult = fn()

      count = phaseResult.count
    } catch {
      case e: SaveToMemSQLException => {
        logError(s"Phase error in pipeline $pipeline_id, preserving rows count", e.exception)
        error = Some(getStackTraceAsString(e.exception))
        count = Some(e.successfullyInserted)
      }
      case NonFatal(e) => {
        logError(s"Phase error in pipeline $pipeline_id", e)
        error = Some(getStackTraceAsString(e))
      }
    }
    if (logger != null) {
      if (trace) {
        logs = Some(logger.getLogEntries)
      }
      // Clear the logger's entries so that each logger only contains log
      // entries for a single batch; this is especially important for extract
      // loggers, where there's only a single extract logger that's shared
      // among all batches.
      logger.clearLogEntries
    }
    val stopTime = System.currentTimeMillis
    Some(PhaseMetricRecord(
      start = startTime,
      stop = stopTime,
      count = count,
      error = error,
      columns = phaseResult.columns,
      records = phaseResult.records,
      logs = logs
    ))
  }

  private[interface] def getPhaseLogger(phaseName: String, trackEntries: Boolean=true): PipelineLogger = {
    new PipelineLogger(s"Pipeline $pipeline_id $phaseName", trackEntries)
  }

  private[interface] def getExtractRecords(df: DataFrame): (Option[List[(String, String)]], Option[List[List[String]]]) = {
    val fields = df.schema.fields.map(field => (field.name, field.dataType.typeName)).toList
    val values: List[List[String]] = df.rdd.map(row => {
      try {
        row.toSeq.map(record => {
          val sb = new StringBuilder()
          record match {
            case null => sb.append("null")
            case bytes: Array[Byte] => {
              // Build up a string with hex encoding such that printable ASCII
              // characters get added as-is but other characters are added as an
              // escape sequence (e.g. \x7f).
              bytes.foreach(b => {
                if (b >= 0x20 && b <= 0x7e) {
                  sb.append(b.toChar)
                } else {
                  sb.append("\\x%02x".format(b))
                }
              })
            }
            case default => sb.append(record.toString)
          }

          sb.toString
        }).toList
      } catch {
        case e: Exception => List(s"Could not get string representation of record: $e")
      }
    }).collect.toList
    (Some(fields), Some(values))
  }

  private[interface] def getTransformRecords(df: DataFrame, truncCount: Long): (Option[List[(String, String)]], Option[List[List[String]]]) = {
    val fields = df.schema.fields.map(field => (field.name, field.dataType.typeName)).toList
    // Create a list of lists, where each inner list represents the values of
    // each column in fieldNames for a given row.
    val values: List[List[String]] = df.take(truncCount.toInt).map(row => {
      try {
        fields.map(field => {
          row.getAs[Any](field._1) match {
            case null => "null"
            case default => default.toString
          }
        })
      } catch {
        case e: Exception => List(s"Could not get string representation of row: $e")
      }
    }).toList
    (Some(fields), Some(values))
  }

  private[interface] def getLoadColumns(): Option[List[(String, String)]] = {
    if (sqlContext.isInstanceOf[MemSQLContext] && pipelineInstance.loadConfig.isInstanceOf[MemSQLLoadConfig]) {
      val memSQLContext = sqlContext.asInstanceOf[MemSQLContext]
      val memSQLLoadConfig = pipelineInstance.loadConfig.asInstanceOf[MemSQLLoadConfig]
      val columnsSchema = memSQLContext.table(memSQLLoadConfig.getTableIdentifier).schema
      Some(columnsSchema.fields.map(field => (field.name, field.dataType.typeName)).toList)
    } else {
      None
    }
  }

  private[interface] def getTaskErrors(batch_id: String): Option[List[TaskErrorRecord]] = {
    if (jobProgressListener == null) {
      return None // scalastyle:ignore
    }

    val maybeJobAndStageIds = jobProgressListener.jobGroupToJobIds.get(batch_id) match {
      case Some(jobIds) => {
        Some(jobIds.toList.flatMap { jobId =>
          jobProgressListener.jobIdToData.get(jobId) match {
            case Some(jobData) => jobData.stageIds.map(x => (jobId, x))
            case None => {
              logDebug(s"Could not find information for job $jobId in pipeline $pipeline_id")
              Seq()
            }
          }
        })
      }
      case None => {
        logDebug(s"Could not find information for batch $batch_id in pipeline $pipeline_id")
        None
      }
    }

    // Keep track of the first lines of error messages we've seen thus far and
    // don't include error messages with the same first line as a message
    // we've already seen; for instance, we will not include two
    // ClassCastException error messages in the same batch.
    val errorMessages = HashSet[String]()
    val errorRecords = maybeJobAndStageIds match {
      case Some(jobAndStageIds) => {
        Some(jobAndStageIds.flatMap { case (jobId, stageId) =>
          jobProgressListener.stageIdToInfo.get(stageId) match {
            case Some(stageInfo) => {
              val attemptId = stageInfo.attemptId
              jobProgressListener.stageIdToData.get((stageId, attemptId)) match {
                case Some(stageData) => {
                  stageData.taskData.filter { case (taskId, taskData) =>
                    taskData.errorMessage match {
                      case Some(error) => {
                        val firstLine = error.split("\n")(0)
                        if (!error.contains("ExecutorLostFailure") && !errorMessages.contains(firstLine)) {

                          errorMessages.add(firstLine)
                          true
                        } else {
                          false
                        }
                      }
                      case None => false
                    }
                  }.map { case (taskId, taskData) =>
                    TaskErrorRecord(jobId.toInt, stageId, taskId, taskData.taskInfo.finishTime, taskData.errorMessage)
                  }
                }
                case None => {
                  logDebug(s"Could not find data for stage $stageId in pipeline $pipeline_id")
                  None
                }
              }
            }
            case None => {
              logDebug(s"Could not find information for stage $stageId in pipeline $pipeline_id")
              None
            }
          }
        })
      }
      case None => None
    }

    errorRecords match {
      case None => None
      case Some(Nil) => None
      case default => default
    }
  }

  private[interface] def enqueueCancelEvent(): Unit = {
    if (currentBatchId != null && currentBatchType != null) {
      val batchCancelledEvent = BatchCancelledEvent(
        batch_id = currentBatchId,
        batch_type = currentBatchType,
        pipeline_id = pipeline_id,
        timestamp = System.currentTimeMillis,
        event_id = UUID.randomUUID.toString)
      pipeline.enqueueMetricRecord(batchCancelledEvent)
    }
  }

  override def ensureStarted(): Unit = {
    try {
      thread.start
    } catch {
      case e: IllegalThreadStateException => {}
    }
  }

  def isAlive(): Boolean = {
    thread.isAlive
  }

  def hasError(): Boolean = {
    error != null
  }

  def stop(): Unit = {
    logDebug(s"Stopping pipeline thread $pipeline_id")
    isStopping.set(true)
    if (currentBatchId != null) {
      sparkContext.cancelJobGroup(currentBatchId)
    }
    thread.interrupt
    thread.join
  }
}
