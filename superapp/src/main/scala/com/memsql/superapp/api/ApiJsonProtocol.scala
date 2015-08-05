package com.memsql.superapp.api

import com.memsql.spark.etl.api.configs._
import spray.json._

object ApiJsonProtocol extends DefaultJsonProtocol {
  def jsonEnum[T <: Enumeration](enu: T) = new JsonFormat[T#Value] {
    def write(obj: T#Value) = JsString(obj.toString)

    def read(json: JsValue) = json match {
      case JsString(txt) => {
        try {
          enu.withName(txt)
        } catch {
          case e: NoSuchElementException => deserializationError(s"expected a value from $enu instead of $txt")
        }
      }
      case default => deserializationError(s"$enu value expected")
    }
  }

  implicit val extractPhaseKindFormat = jsonEnum(ExtractPhaseKind)
  implicit val transformPhaseKindFormat = jsonEnum(TransformPhaseKind)
  implicit val loadPhaseKindFormat = jsonEnum(LoadPhaseKind)

  implicit def phaseFormat[T :JsonFormat] = jsonFormat2(Phase.apply[T])

  implicit val pipelineConfigFormat = jsonFormat4(PipelineConfig)

  implicit val pipelineStateFormat = jsonEnum(PipelineState)
  implicit val pipelineFormat = jsonFormat6(Pipeline)
}
