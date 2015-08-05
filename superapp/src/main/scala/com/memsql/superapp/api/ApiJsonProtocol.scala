package com.memsql.superapp.api

import com.memsql.spark.etl.api.configs._
import spray.json._

object ApiJsonProtocol extends JsonEnumProtocol {
  implicit val extractPhaseKindFormat = jsonEnum(ExtractPhaseKind)
  implicit val transformPhaseKindFormat = jsonEnum(TransformPhaseKind)
  implicit val loadPhaseKindFormat = jsonEnum(LoadPhaseKind)

  implicit def phaseFormat[T :JsonFormat] = jsonFormat2(Phase.apply[T])

  implicit val pipelineConfigFormat = jsonFormat4(PipelineConfig)

  implicit val pipelineStateFormat = jsonEnum(PipelineState)
  implicit val pipelineFormat = jsonFormat6(Pipeline)
}
