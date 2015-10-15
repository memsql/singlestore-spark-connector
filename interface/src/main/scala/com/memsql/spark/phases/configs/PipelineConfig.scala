package com.memsql.spark.etl.api.configs

import spray.json._

object PipelineConfigVersion {
  val CurrentPipelineConfigVersion = 1
}

case class Phase[T](kind: T, config: JsValue)

object ExtractPhaseKind extends Enumeration {
  type ExtractPhaseKind = Value
  val ZookeeperManagedKafka, Kafka, TestLines, User = Value
}
import com.memsql.spark.etl.api.configs.ExtractPhaseKind._

object TransformPhaseKind extends Enumeration {
  type TransformPhaseKind = Value
  val Json, Csv, User = Value
}
import com.memsql.spark.etl.api.configs.TransformPhaseKind._

object LoadPhaseKind extends Enumeration {
  type LoadPhaseKind = Value
  val MemSQL = Value
}
import com.memsql.spark.etl.api.configs.LoadPhaseKind._

// The PipelineConfig object contains configuration for all phases of an
// ETL pipeline.
case class PipelineConfig(var extract: Phase[ExtractPhaseKind],
                          var transform: Phase[TransformPhaseKind],
                          var load: Phase[LoadPhaseKind],
                          var enable_checkpointing: Option[Boolean] = None,
                          var config_version: Int = PipelineConfigVersion.CurrentPipelineConfigVersion)
