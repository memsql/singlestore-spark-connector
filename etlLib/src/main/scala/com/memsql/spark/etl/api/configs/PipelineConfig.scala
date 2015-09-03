package com.memsql.spark.etl.api.configs

import spray.json._

object PipelineConfigVersion {
  val CurrentPipelineConfigVersion = 1
}
import PipelineConfigVersion._

abstract class PhaseConfig

case class Phase[T](kind: T, config: JsValue)

object ExtractPhaseKind extends Enumeration {
  type ExtractPhaseKind = Value
  val Kafka, TestLines, User = Value
}
import ExtractPhaseKind._

object TransformPhaseKind extends Enumeration {
  type TransformPhaseKind = Value
  val Json, User = Value
}
import TransformPhaseKind._

object LoadPhaseKind extends Enumeration {
  type LoadPhaseKind = Value
  val MemSQL, User = Value
}
import LoadPhaseKind._

// The PipelineConfig object contains configuration for all phases of an
// ETL pipeline.
case class PipelineConfig(var extract: Phase[ExtractPhaseKind],
                          var transform: Phase[TransformPhaseKind],
                          var load: Phase[LoadPhaseKind],
                          var config_version: Int = PipelineConfigVersion.CurrentPipelineConfigVersion)
