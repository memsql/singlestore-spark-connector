package com.memsql.spark.interface.api
import com.memsql.spark.interface.api.PipelineEventType.PipelineEventType

object PipelineBatchType extends Enumeration {
  type PipelineBatchType = Value
  val Normal, Traced = Value
}

import PipelineBatchType._

case class TaskErrorRecord(job_id: Int, stage_id: Int, task_id: Long, finish_time: Long, error: Option[String])

case class PhaseMetricRecord(start: Long,
                             stop: Long,
                             count: Option[Long],
                             error: Option[String],
                             columns: Option[List[(String, String)]],
                             records: Option[List[List[String]]],
                             logs: Option[List[String]])

object PipelineEventType extends Enumeration {
  type PipelineEventType = Value
  val BatchStart, BatchEnd, PipelineStart, PipelineEnd = Value
}

abstract class PipelineEvent {
  val pipeline_id: String
  val timestamp: Long
  val event_type: PipelineEventType
}

case class BatchStartEvent(batch_id: String,
                           batch_type: PipelineBatchType,
                           pipeline_id: String,
                           timestamp: Long,
                           event_type: PipelineEventType = PipelineEventType.BatchStart) extends PipelineEvent

case class BatchEndEvent(batch_id: String,
                         batch_type: PipelineBatchType,
                         pipeline_id: String,
                         timestamp: Long,
                         success: Boolean,
                         task_errors: Option[List[TaskErrorRecord]],
                         extract: Option[PhaseMetricRecord],
                         transform: Option[PhaseMetricRecord],
                         load: Option[PhaseMetricRecord],
                         event_type: PipelineEventType = PipelineEventType.BatchEnd) extends PipelineEvent

case class PipelineStartEvent(pipeline_id: String,
                              timestamp: Long,
                              event_type: PipelineEventType = PipelineEventType.PipelineStart) extends PipelineEvent

case class PipelineEndEvent(pipeline_id: String,
                            timestamp: Long,
                            event_type: PipelineEventType = PipelineEventType.PipelineEnd) extends PipelineEvent
