package com.memsql.superapp.api

import com.memsql.spark.etl.api._
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
  case class PipelinePut(pipeline_id: String, jar: String, main_class: String, config: PipelineConfig = null)
  case class PipelineUpdate(pipeline_id: String, state: PipelineState = null, config: PipelineConfig = null, error: Option[String] = None, _validate: Boolean = false)
  case class PipelineDelete(pipeline_id: String)
  implicit val timeout = Timeout(5.seconds)
}

class ApiActor(config: Config) extends Actor {
  import ApiActor._

  private var pipelines = new mutable.ListBuffer[Pipeline]()

  private def normalizeConfig(config: PipelineConfig): PipelineConfig = {
    var pipelineConfig = config
    if (pipelineConfig == null) {
      pipelineConfig = PipelineConfig(None, None, None)
    }
    if (pipelineConfig.extract.isEmpty) {
      pipelineConfig.extract = Some(Phase[ExtractPhaseKind](
        ExtractPhaseKind.User,
        ExtractPhase.writeConfig(ExtractPhaseKind.User, UserExtractConfig(""))))
    }
    if (pipelineConfig.transform.isEmpty) {
      pipelineConfig.transform = Some(Phase[TransformPhaseKind](
        TransformPhaseKind.User,
        TransformPhase.writeConfig(TransformPhaseKind.User, UserTransformConfig(""))))
    }
    if (pipelineConfig.load.isEmpty) {
      pipelineConfig.load = Some(Phase[LoadPhaseKind](
        LoadPhaseKind.User,
        LoadPhase.writeConfig(LoadPhaseKind.User, UserLoadConfig(""))))
    }
    pipelineConfig
  }

  override def receive: Receive = {
    case Ping => sender ! "pong"
    case PipelineQuery => sender ! pipelines.toList
    case PipelineGet(pipeline_id) => {
      pipelines.find(_.pipeline_id == pipeline_id) match {
        case Some(pipeline) => sender ! Success(pipeline)
        case None => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case PipelinePut(pipeline_id, jar, main_class, config) => {
      var pipelineConfig = normalizeConfig(config)
      // Assert that the phase configs, which are stored as JSON blobs, can be
      // deserialized properly.
      try {
        ExtractPhase.readConfig(pipelineConfig.extract.get.kind, pipelineConfig.extract.get.config)
        TransformPhase.readConfig(pipelineConfig.transform.get.kind, pipelineConfig.transform.get.config)
        LoadPhase.readConfig(pipelineConfig.load.get.kind, pipelineConfig.load.get.config)
        pipelines.find(_.pipeline_id == pipeline_id) match {
          case p: Some[Pipeline] => sender ! Failure(ApiException(s"pipeline with id $pipeline_id already exists"))
          case _ => {
            pipelines += Pipeline(pipeline_id, RUNNING, jar, main_class, pipelineConfig)
            sender ! Success(true)
          }
        }
      } catch {
        case e: DeserializationException => sender ! Failure(ApiException(s"config does not validate: $e"))
      }
    }
    case PipelineUpdate(pipeline_id, state, config, error, _validate) => {
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

          try {
            if (config != null) {
              var pipelineConfig = normalizeConfig(config)
              // Assert that the phase configs, which are stored as JSON blobs,
              // can be deserialized properly.
              ExtractPhase.readConfig(config.extract.get.kind, config.extract.get.config)
              TransformPhase.readConfig(config.transform.get.kind, config.transform.get.config)
              LoadPhase.readConfig(config.load.get.kind, config.load.get.config)
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
