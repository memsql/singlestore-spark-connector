package com.memsql.spark.interface.api

import com.memsql.spark.etl.api.configs.PipelineConfig
import com.memsql.spark.interface.util.BoundedQueue

object PipelineState extends Enumeration {
  type PipelineState = Value
  val RUNNING, STOPPED, ERROR = Value
}

import PipelineState._

case class Pipeline(pipeline_id: String,
                    state: PipelineState,
                    jar: String,
                    batch_interval: Long,
                    config: PipelineConfig,
                    last_updated: Long,
                    error: Option[String] = None) {
  val MAX_METRICS_QUEUE_SIZE = 1000
  private[interface] var metricsQueue = new BoundedQueue[PipelineMetricRecord](MAX_METRICS_QUEUE_SIZE)

  private[interface] def enqueueMetricRecord(records: PipelineMetricRecord*) = {
    metricsQueue.enqueue(records: _*)
  }
}
