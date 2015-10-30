package com.memsql.spark.interface.api

import com.memsql.spark.etl.api.configs._
import com.memsql.spark.etl.utils.JsonEnumProtocol
import spray.json._

object ApiJsonProtocol extends JsonEnumProtocol {
  implicit val extractPhaseKindFormat = jsonEnum(ExtractPhaseKind)
  implicit val transformPhaseKindFormat = jsonEnum(TransformPhaseKind)
  implicit val loadPhaseKindFormat = jsonEnum(LoadPhaseKind)

  implicit def phaseFormat[T: JsonFormat]: RootJsonFormat[Phase[T]] = jsonFormat2(Phase.apply[T])

  implicit val pipelineConfigFormat = jsonFormat5(PipelineConfig)

  implicit val pipelineBatchTypeFormat = jsonEnum(PipelineBatchType)
  implicit val phaseMetricRecordFormat = jsonFormat7(PhaseMetricRecord)
  implicit val taskErrorRecordFormat = jsonFormat5(TaskErrorRecord)

  implicit val pipelineEventTypeFormat = jsonEnum(PipelineEventType)
  implicit val batchStartEventFormat = jsonFormat6(BatchStartEvent)
  implicit val batchEndEventFormat = jsonFormat11(BatchEndEvent)
  implicit val pipelineStartEventFormat = jsonFormat4(PipelineStartEvent)
  implicit val pipelineEndEventFormat = jsonFormat4(PipelineEndEvent)

  implicit val pipelineStateFormat = jsonEnum(PipelineState)
  implicit val pipelineThreadStateFormat = jsonEnum(PipelineThreadState)

  val basePipelineFormat = jsonFormat(Pipeline.apply, "pipeline_id", "state", "batch_interval", "config", "last_updated", "error")

  implicit object pipelineEventFormat extends RootJsonFormat[PipelineEvent] {
    def write(e: PipelineEvent): JsValue = e match {
      case batchStartEvent: BatchStartEvent => batchStartEvent.toJson
      case batchEndEvent: BatchEndEvent => batchEndEvent.toJson
      case pipelineStartEvent: PipelineStartEvent => pipelineStartEvent.toJson
      case pipelineEndEvent: PipelineEndEvent => pipelineEndEvent.toJson
    }

    def read(value: JsValue): PipelineEvent = null
  }

  implicit object pipelineFormat extends RootJsonFormat[Pipeline] {
    def write(p: Pipeline): JsValue =
      JsObject(
        basePipelineFormat.write(p).asJsObject.fields + ("trace_batch_count" -> p.traceBatchCount.toJson)
          + ("thread_state" -> p.thread_state.toJson)
      )

    // Note: This method doesn't support reading in traceBatchCount
    def read(value: JsValue): Pipeline = basePipelineFormat.read(value)
  }
}
