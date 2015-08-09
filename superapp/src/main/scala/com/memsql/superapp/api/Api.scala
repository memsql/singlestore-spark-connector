package com.memsql.superapp.api

import akka.actor.Actor
import akka.util.Timeout
import scala.concurrent.duration._
import com.memsql.superapp.Config
import com.memsql.superapp.util.BaseException
import com.memsql.spark.etl.api.configs._
import spray.json._

import scala.collection.mutable
import scala.util.{Failure, Success}

import PipelineState._
import ExtractPhaseKind._
import TransformPhaseKind._
import LoadPhaseKind._

case class ApiException(message: String) extends BaseException(message: String)

object ApiActor {
  case object Ping
  case object PipelineQuery
  case class PipelineGet(pipeline_id: String)
  case class PipelinePut(pipeline_id: String, jar: String, batchInterval: Long, config: PipelineConfig)
  case class PipelineUpdate(pipeline_id: String, state: PipelineState = null, batchInterval: Long = 0,
                            config: PipelineConfig = null, error: Option[String] = None, _validate: Boolean = false)
  case class PipelineDelete(pipeline_id: String)
  implicit val timeout = Timeout(5.seconds)
}

class ApiActor(config: Config) extends Actor {
  import ApiActor._

  private var pipelines = new mutable.ListBuffer[Pipeline]()

  override def receive: Receive = {
    case Ping => sender ! "pong"
    case PipelineQuery => sender ! pipelines.toList
    case PipelineGet(pipeline_id) => {
      pipelines.find(_.pipeline_id == pipeline_id) match {
        case Some(pipeline) => sender ! Success(pipeline)
        case None => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case PipelinePut(pipeline_id, jar, batchInterval, config) => {
      // Assert that the phase configs, which are stored as JSON blobs, can be
      // deserialized properly.
      try {
        ExtractPhase.readConfig(config.extract.kind, config.extract.config)
        TransformPhase.readConfig(config.transform.kind, config.transform.config)
        LoadPhase.readConfig(config.load.kind, config.load.config)
        pipelines.find(_.pipeline_id == pipeline_id) match {
          case p: Some[Pipeline] => sender ! Failure(ApiException(s"pipeline with id $pipeline_id already exists"))
          case _ => {
            pipelines += Pipeline(pipeline_id, RUNNING, jar, batchInterval, config)
            sender ! Success(true)
          }
        }
      } catch {
        case e: DeserializationException => sender ! Failure(ApiException(s"config does not validate: $e"))
      }
    }
    case PipelineUpdate(pipeline_id, state, batchInterval, config, error, _validate) => {
      pipelines.find(_.pipeline_id == pipeline_id) match {
        case p: Some[Pipeline] => {
          val pipeline = p.get
          val oldState = pipeline.state
          val oldConfig = pipeline.config
          var updated = false

          if (state != null) {
            pipeline.state = (pipeline.state, state, _validate) match {
              case (_, _, false) => state
              case (RUNNING, STOPPED, _) => state
              case (STOPPED, RUNNING, _) => state
              case default => pipeline.state
            }
            if (oldState != pipeline.state) {
              pipeline.error = error
              updated = true
            }
          }

          if (batchInterval > 0) {
            pipeline.batch_interval = batchInterval
            updated = true
          }

          try {
            if (config != null) {
              // Assert that the phase configs, which are stored as JSON blobs,
              // can be deserialized properly.
              ExtractPhase.readConfig(config.extract.kind, config.extract.config)
              TransformPhase.readConfig(config.transform.kind, config.transform.config)
              LoadPhase.readConfig(config.load.kind, config.load.config)
              pipeline.config = config
              updated = oldConfig != pipeline.config
            }
            sender ! Success(updated)
          } catch {
            case e: DeserializationException => sender ! Failure(ApiException(s"config does not validate: $e"))
          }
        }
        case _ => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case PipelineDelete(pipeline_id) => {
      pipelines.find(_.pipeline_id == pipeline_id) match {
        case p: Some[Pipeline] => {
          pipelines -= p.get
          sender ! Success(true)
        }
        case _ => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case default => sender ! ApiException("invalid api message type")
  }
}
