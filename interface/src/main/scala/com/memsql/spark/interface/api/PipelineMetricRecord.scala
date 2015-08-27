package com.memsql.spark.interface.api

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

case class PipelineMetricRecord(batch_id: String,
                                batch_type: PipelineBatchType,
                                pipeline_id: String,
                                timestamp: Long,
                                success: Boolean,
                                task_errors: Option[List[TaskErrorRecord]],
                                extract: Option[PhaseMetricRecord],
                                transform: Option[PhaseMetricRecord],
                                load: Option[PhaseMetricRecord])
