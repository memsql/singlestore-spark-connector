package com.memsql.spark.interface.api

case class PhaseMetricRecord(start: Long,
                             stop: Long,
                             count: Option[Long],
                             error: Option[String],
                             records: Option[List[String]],
                             logs: Option[List[String]])

case class PipelineMetricRecord(pipeline_id: String,
                                timestamp: Long,
                                success: Boolean,
                                extract: Option[PhaseMetricRecord],
                                transform: Option[PhaseMetricRecord],
                                load: Option[PhaseMetricRecord])
