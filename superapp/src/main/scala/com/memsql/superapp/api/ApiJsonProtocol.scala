package com.memsql.superapp.api

import com.memsql.spark.etl.api._
import spray.json._
import PipelineState._
import PipelineExtractType._
import PipelineTransformType._
import PipelineLoadType._

object ApiJsonProtocol extends DefaultJsonProtocol {
  implicit object pipelineExtractConfigJsonFormat extends RootJsonFormat[PipelineExtractConfig] {
    override def read(value: JsValue): PipelineExtractConfig = {
      value.asJsObject.getFields("type", "config") match {
        case Seq(JsString(extract_type), JsString(config)) => {
          var extractTypeEnum: PipelineExtractType = null
          try {
            extractTypeEnum = PipelineExtractType.withName(extract_type)
          } catch {
            case e: NoSuchElementException => deserializationError(s"pipeline extract type $extract_type does not exist")
          }
          var extractConfigData: PipelineExtractConfigData = null
          extractTypeEnum match {
            case PipelineExtractType.KAFKA => {
              extractConfigData = config.parseJson.convertTo[PipelineKafkaExtractConfigData]
            }
            case PipelineExtractType.USER => {
              extractConfigData = config.parseJson.convertTo[PipelineUserExtractConfigData]
            }
          }
          PipelineExtractConfig(extractTypeEnum, extractConfigData)
        }
        case _ => deserializationError("pipeline extract config expected")
      }
    }

    override def write(obj: PipelineExtractConfig): JsValue = {
      val configStr = obj.config match {
        case kafka: PipelineKafkaExtractConfigData => JsString(kafka.toJson.toString)
        case user: PipelineUserExtractConfigData => JsString(user.toJson.toString)
      }
      JsObject(
        "type" -> JsString(obj.`type`.toString),
        "config" -> configStr
      )
    }
  }

  implicit val pipelineKafkaExtractConfigDataFormat = jsonFormat3(PipelineKafkaExtractConfigData)
  implicit val pipelineUserExtractConfigDataFormat = jsonFormat1(PipelineUserExtractConfigData)

  implicit object pipelineTransformConfigJsonFormat extends RootJsonFormat[PipelineTransformConfig] {
    override def read(value: JsValue): PipelineTransformConfig = {
      value.asJsObject.getFields("type", "config") match {
        case Seq(JsString(transform_type), JsString(config)) => {
          var transformTypeEnum: PipelineTransformType = null
          try {
            transformTypeEnum = PipelineTransformType.withName(transform_type)
          } catch {
            case e: NoSuchElementException => deserializationError(s"pipeline transform type $transform_type does not exist")
          }
          var transformConfigData: PipelineTransformConfigData = null
          transformTypeEnum match {
            case PipelineTransformType.USER => {
              transformConfigData = config.parseJson.convertTo[PipelineUserTransformConfigData]
            }
          }
          PipelineTransformConfig(transformTypeEnum, transformConfigData)
        }
        case _ => deserializationError("pipeline transform config expected")
      }
    }

    override def write(obj: PipelineTransformConfig): JsValue = {
      val configStr = obj.config match {
        case user: PipelineUserTransformConfigData => JsString(user.toJson.toString)
      }
      JsObject(
        "type" -> JsString(obj.`type`.toString),
        "config" -> configStr
      )
    }
  }

  implicit val pipelineUserTransformConfigDataFormat = jsonFormat1(PipelineUserTransformConfigData)

  implicit object pipelineLoadConfigJsonFormat extends RootJsonFormat[PipelineLoadConfig] {
    override def read(value: JsValue): PipelineLoadConfig = {
      value.asJsObject.getFields("type", "config") match {
        case Seq(JsString(load_type), JsString(config)) => {
          var loadTypeEnum: PipelineLoadType = null
          try {
            loadTypeEnum = PipelineLoadType.withName(load_type)
          } catch {
            case e: NoSuchElementException => deserializationError(s"pipeline load type $load_type does not exist")
          }
          var loadConfigData: PipelineLoadConfigData = null
          loadTypeEnum match {
            case PipelineLoadType.MEMSQL => {
              loadConfigData = config.parseJson.convertTo[PipelineMemSQLLoadConfigData]
            }
            case PipelineLoadType.USER => {
              loadConfigData = config.parseJson.convertTo[PipelineUserLoadConfigData]
            }
          }
          PipelineLoadConfig(loadTypeEnum, loadConfigData)
        }
        case _ => deserializationError("pipeline load config expected")
      }
    }

    override def write(obj: PipelineLoadConfig): JsValue = {
      val configStr = obj.config match {
        case memsql: PipelineMemSQLLoadConfigData => JsString(memsql.toJson.toString)
        case user: PipelineUserLoadConfigData => JsString(user.toJson.toString)
      }
      JsObject(
        "type" -> JsString(obj.`type`.toString),
        "config" -> configStr
      )
    }
  }

  implicit val pipelineMemSQLLoadConfigDataFormat = jsonFormat2(PipelineMemSQLLoadConfigData)
  implicit val pipelineUserLoadConfigDataFormat = jsonFormat1(PipelineUserLoadConfigData)

  implicit val pipelineConfigFormat = jsonFormat4(PipelineConfig)

  implicit object pipelineJsonFormat extends RootJsonFormat[Pipeline] {
    override def read(value: JsValue): Pipeline = {
      value.asJsObject.getFields("pipeline_id", "state", "jar", "main_class", "config", "error") match {
        case Seq(JsString(pipeline_id), JsString(state), JsString(jar), JsString(main_class), JsString(config), error) => {
          var pipelineConfig: PipelineConfig = config.parseJson.convertTo[PipelineConfig]
          var stateEnum: PipelineState = null
          try {
            stateEnum = PipelineState.withName(state)
          } catch {
            case e: NoSuchElementException => deserializationError(s"pipeline state $state does not exist")
          }
          var errorStr = error match {
            case str: JsString => str.convertTo[String]
            case JsNull => null
            case _ => deserializationError("error must be a string or null")
          }
          Pipeline(pipeline_id, stateEnum, jar, main_class, pipelineConfig, errorStr)
        }
        case _ => deserializationError("pipeline expected")
      }
    }

    override def write(obj: Pipeline): JsValue = {
      JsObject(
        "pipeline_id" -> JsString(obj.pipeline_id),
        "state" -> JsString(obj.state.toString),
        "active" -> JsBoolean(obj.state == PipelineState.RUNNING),
        "jar" -> JsString(obj.jar),
        "main_class" -> JsString(obj.main_class),
        "config" -> JsString(obj.config.toJson.toString),
        "error" -> (if (obj.error == null) JsNull else JsString(obj.error))
      )
    }
  }
}
