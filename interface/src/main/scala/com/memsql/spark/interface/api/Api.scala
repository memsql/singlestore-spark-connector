package com.memsql.spark.interface.api

import akka.actor.Actor.Receive
import akka.actor.{ActorRef, Actor}
import akka.util.Timeout
import com.memsql.spark.interface.util.{Clock, BaseException}
import com.memsql.spark.phases.configs.ExtractPhase
import com.memsql.spark.etl.api.configs.TransformPhase
import scala.concurrent.duration._
import com.memsql.spark.etl.api.configs._
import spray.json._

import scala.util.control.NonFatal
import scala.util.{Failure, Success}
import com.memsql.spark.etl.api.UserExtractConfig
import com.memsql.spark.etl.api.UserTransformConfig

import PipelineState._

case class ApiException(message: String) extends BaseException(message: String)

object ApiActor {
  case object Ping
  case object PipelineQuery
  case class PipelineGet(pipeline_id: String)
  case class PipelinePut(pipeline_id: String, batch_interval: Long, config: PipelineConfig)
  case class PipelineUpdate(pipeline_id: String, state: Option[PipelineState] = None,
                            batch_interval: Option[Long] = None,
                            config: Option[PipelineConfig] = None,
                            trace_batch_count: Option[Int] = None,
                            error: Option[String] = None, _validate: Boolean = false)
  case class PipelineMetrics(pipeline_id: String, last_timestamp: Option[Long])
  case class PipelineTraceBatchDecrement(pipeline_id: String)
  case class PipelineDelete(pipeline_id: String)
  implicit val timeout = Timeout(5.seconds)
}

class ApiActor extends Actor with ApiService {
  override def receive: Receive = handleMessage
}

trait ApiService {
  import ApiActor._
  def sender: ActorRef

  private var pipelines = Map[String, Pipeline]()

  private[interface] def clock = new Clock()

  def handleMessage: Receive = {
    case Ping => sender ! "pong"
    case PipelineQuery => sender ! pipelines.values.toList
    case PipelineGet(pipeline_id) => {
      pipelines.get(pipeline_id) match {
        case Some(pipeline) => sender ! Success(pipeline)
        case None => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case PipelinePut(pipeline_id, batch_interval, config) => {
      try {
        pipelines.get(pipeline_id) match {
          case p: Some[Pipeline] => sender ! Failure(ApiException(s"pipeline with id $pipeline_id already exists"))
          case _ => {
            if (config.extract.kind == ExtractPhaseKind.User) {
              val extractConfig = ExtractPhase.readConfig(config.extract.kind, config.extract.config)
              val className = extractConfig.asInstanceOf[UserExtractConfig].class_name
              Class.forName(className)
            }
            if (config.transform.kind == TransformPhaseKind.User) {
              val transformConfig = TransformPhase.readConfig(config.transform.kind, config.transform.config)
              val className = transformConfig.asInstanceOf[UserTransformConfig].class_name
              Class.forName(className)
            }
            pipelines = pipelines + (pipeline_id -> Pipeline(pipeline_id, RUNNING, batch_interval, config, clock.currentTimeMillis))
            sender ! Success(true)
          }
        }
      } catch {
        case e: ApiException => sender ! Failure(e)
        case e: ClassNotFoundException => sender ! Failure(ApiException(e.toString))
        case NonFatal(e) => sender ! Failure(ApiException(s"unexpected exception: $e"))
      }
    }
    case PipelineUpdate(pipeline_id, state, batch_interval, config, trace_batch_count, error, _validate) => {
      pipelines.get(pipeline_id) match {
        case Some(pipeline) => {
          var updated = false
          var newState = pipeline.state
          var newBatchInterval = pipeline.batch_interval
          var newConfig = pipeline.config
          var newError = pipeline.error
          var newTraceBatchCount = pipeline.traceBatchCount

          try {
            if (state.isDefined) {
              newState = (pipeline.state, state.get, _validate) match {
                case (_, _, false) => state.get
                case (RUNNING, STOPPED, _) => state.get
                case (STOPPED | ERROR, RUNNING, _) => state.get
                case (prev, next, _) if prev == next => state.get
                case (prev, next, _) => throw new ApiException(s"cannot update state from $prev to $next")
              }

              updated |= newState != pipeline.state
            }

            if (batch_interval.isDefined) {
              newBatchInterval = batch_interval.get
              updated |= newBatchInterval != pipeline.batch_interval
            }

            if (config.isDefined) {
              newConfig = config.get

              if (newConfig.extract.kind == ExtractPhaseKind.User) {
                val extractConfig = ExtractPhase.readConfig(newConfig.extract.kind, newConfig.extract.config)
                val className = extractConfig.asInstanceOf[UserExtractConfig].class_name
                Class.forName(className)
              }
              if (newConfig.transform.kind == TransformPhaseKind.User) {
                val transformConfig = TransformPhase.readConfig(newConfig.transform.kind, newConfig.transform.config)
                val className = transformConfig.asInstanceOf[UserTransformConfig].class_name
                Class.forName(className)
              }
              updated |= newConfig != pipeline.config
            }

            if (error.isDefined) {
              if (error.get.isEmpty) {
                // We treat empty string errors as equivalent to no errors.
                newError = None
              } else {
                newError = error
              }
              updated |= newError != pipeline.error
            }

            // NOTE: we don't update the pipeline if only the trace_batch_count is changed
            if (trace_batch_count.isDefined) {
              newTraceBatchCount = trace_batch_count.get
            }

            // update all fields in the pipeline and respond with success
            if (updated) {
              val newLastUpdated = clock.currentTimeMillis
              val newPipeline = Pipeline(pipeline_id, state=newState, batch_interval=newBatchInterval,
                                         last_updated=newLastUpdated, config=newConfig, error=newError)
              newPipeline.traceBatchCount = newTraceBatchCount
              newPipeline.metricsQueue = pipeline.metricsQueue
              pipelines = pipelines + (pipeline_id -> newPipeline)
              sender ! Success(true)
            } else if (newTraceBatchCount != pipeline.traceBatchCount) {
              // in the case where only the batch count has changed, update it in place
              pipeline.traceBatchCount = newTraceBatchCount
              sender ! Success(true)
            } else {
              sender ! Success(false)
            }
          } catch {
            case e: ApiException => sender ! Failure(e)
            case e: ClassNotFoundException => sender ! Failure(ApiException(e.toString))
            case NonFatal(e) => sender ! Failure(ApiException(s"unexpected exception: $e"))
          }
        }
        case _ => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case PipelineTraceBatchDecrement(pipeline_id) => {
      pipelines.get(pipeline_id) match {
        case Some(pipeline) => {
          var updated = false
          if (pipeline.traceBatchCount > 0) {
            pipeline.traceBatchCount = pipeline.traceBatchCount - 1
            updated = true
          }
          sender ! Success(updated)
        }
        case _ => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case PipelineMetrics(pipeline_id, last_timestamp) => {
      pipelines.get(pipeline_id) match {
        case Some(pipeline) => {
          val lastTimestamp = last_timestamp match {
            case Some(t) => t
            case None => 0
          }
          val metricRecords = pipeline.metricsQueue.filter(_.timestamp > lastTimestamp).toList
          sender ! Success(metricRecords)
        }
        case _ => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case PipelineDelete(pipeline_id) => {
      pipelines.get(pipeline_id) match {
        case Some(pipeline) => {
          pipelines = pipelines -- Set(pipeline_id)
          sender ! Success(true)
        }
        case _ => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case default => sender ! ApiException("invalid api message type")
  }
}
