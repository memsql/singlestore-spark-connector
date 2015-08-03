package com.memsql.superapp.api

import com.memsql.spark.etl.api._
import akka.actor.Actor
import akka.util.Timeout
import scala.concurrent.duration._
import com.memsql.superapp.Config
import com.memsql.superapp.util.BaseException
import com.memsql.spark.etl.api.PipelineConfig
import spray.json._

import scala.collection.mutable
import scala.util.{Failure, Success}

import PipelineState._
import PipelineExtractType._
import PipelineTransformType._
import PipelineLoadType._

case class ApiException(message: String) extends BaseException(message: String)

object ApiActor {
  case object Ping
  case object PipelineQuery
  case class PipelineGet(pipeline_id: String)
  case class PipelinePut(pipeline_id: String, jar: String, main_class: String, config: PipelineConfig = null)
  case class PipelineUpdate(pipeline_id: String, state: PipelineState = null, config: PipelineConfig = null, error: String = null, _validate: Boolean = false)
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
    case PipelinePut(pipeline_id, jar, main_class, config) => {
      var pipelineConfig = config
      if (pipelineConfig == null) {
        pipelineConfig = PipelineConfig(None, None, None)
      }
      if (pipelineConfig.extract_config.isEmpty) {
        pipelineConfig.extract_config = Some(PipelineExtractConfig(
          PipelineExtractType.USER, PipelineUserExtractConfigData("")))
      }
      if (pipelineConfig.transform_config.isEmpty) {
        pipelineConfig.transform_config = Some(PipelineTransformConfig(
          PipelineTransformType.USER, PipelineUserTransformConfigData("")))
      }
      if (pipelineConfig.load_config.isEmpty) {
        pipelineConfig.load_config = Some(PipelineLoadConfig(
          PipelineLoadType.USER, PipelineUserLoadConfigData("")))
      }
      pipelines.find(_.pipeline_id == pipeline_id) match {
        case p: Some[Pipeline] => sender ! Failure(ApiException(s"pipeline with id $pipeline_id already exists"))
        case _ => {
          pipelines += Pipeline(pipeline_id, RUNNING, jar, main_class, pipelineConfig)
          sender ! Success(true)
        }
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

          if (config != null) {
            pipeline.config = config
            updated = oldConfig != pipeline.config
          }
          sender ! Success(updated)
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
