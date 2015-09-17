package com.memsql.spark.etl.api.configs

import com.memsql.spark.etl.api.{UserTransformConfig, UserConfig, PhaseConfig}
import spray.json._
import TransformPhaseKind._

case class JsonTransformConfig(column_name: String) extends PhaseConfig

object TransformPhase extends DefaultJsonProtocol {
  val jsonConfigFormat = jsonFormat1(JsonTransformConfig)
  val userConfigFormat = jsonFormat2(UserTransformConfig)

  def readConfig(kind: TransformPhaseKind, config: JsValue): PhaseConfig = {
    kind match {
      case TransformPhaseKind.Json => jsonConfigFormat.read(config)
      case TransformPhaseKind.User => userConfigFormat.read(config)
    }
  }

  def writeConfig(kind: TransformPhaseKind, config: PhaseConfig): JsValue = {
    kind match {
      case TransformPhaseKind.Json => jsonConfigFormat.write(config.asInstanceOf[JsonTransformConfig])
      case TransformPhaseKind.User => userConfigFormat.write(config.asInstanceOf[UserTransformConfig])
    }
  }
}
