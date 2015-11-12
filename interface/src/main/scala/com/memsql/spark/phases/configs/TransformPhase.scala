package com.memsql.spark.etl.api.configs

import com.memsql.spark.etl.api.{UserTransformConfig, PhaseConfig}
import spray.json._
import TransformPhaseKind._

import com.memsql.spark.phases.{PythonTransformConfig, CSVTransformerConfig, JsonTransformConfig}

object TransformPhase extends DefaultJsonProtocol {
  val jsonConfigFormat = jsonFormat1(JsonTransformConfig)
  val csvTransformerConfigFormat = jsonFormat5(CSVTransformerConfig)
  val userConfigFormat = jsonFormat2(UserTransformConfig)
  val pythonConfigFormat = jsonFormat2(PythonTransformConfig)

  def readConfig(kind: TransformPhaseKind, config: JsValue): PhaseConfig = {
    kind match {
      case TransformPhaseKind.Json => jsonConfigFormat.read(config)
      case TransformPhaseKind.Csv => csvTransformerConfigFormat.read(config)
      case TransformPhaseKind.User => userConfigFormat.read(config)
      case TransformPhaseKind.Python => pythonConfigFormat.read(config)
    }
  }

  def writeConfig(kind: TransformPhaseKind, config: PhaseConfig): JsValue = {
    kind match {
      case TransformPhaseKind.Json => jsonConfigFormat.write(config.asInstanceOf[JsonTransformConfig])
      case TransformPhaseKind.Csv => csvTransformerConfigFormat.write(config.asInstanceOf[CSVTransformerConfig])
      case TransformPhaseKind.User => userConfigFormat.write(config.asInstanceOf[UserTransformConfig])
      case TransformPhaseKind.Python => pythonConfigFormat.write(config.asInstanceOf[PythonTransformConfig])
    }
  }
}
