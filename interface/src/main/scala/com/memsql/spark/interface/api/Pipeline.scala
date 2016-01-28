package com.memsql.spark.interface.api

import com.memsql.spark.etl.api.configs._
import com.memsql.spark.interface.util.BoundedQueue
import com.memsql.spark.phases.configs.ExtractPhase
import spray.json.DeserializationException

object PipelineState extends Enumeration {
  type PipelineState = Value
  val RUNNING, STOPPED, FINISHED, ERROR = Value
}

object PipelineThreadState extends Enumeration {
  type PipelineThreadState = Value
  val THREAD_RUNNING, THREAD_STOPPED = Value
}

import PipelineState._

case class Pipeline(pipeline_id: String,
                    state: PipelineState,
                    single_step: Boolean,
                    batch_interval: Long,
                    config: PipelineConfig,
                    last_updated: Long,
                    error: Option[String] = None) {
  Pipeline.validate(single_step, batch_interval, config)

  val MAX_METRICS_QUEUE_SIZE = 1000
  @volatile private[interface] var metricsQueue = new BoundedQueue[PipelineEvent](MAX_METRICS_QUEUE_SIZE)
  @volatile private[interface] var traceBatchCount = 0
  @volatile private[interface] var thread_state = PipelineThreadState.THREAD_STOPPED
  @volatile private[interface] var jobGroupId: Option[String] = None

  private[interface] def enqueueMetricRecord(records: PipelineEvent*) = {
    metricsQueue.enqueue(records: _*)
  }
}

object Pipeline {
  def validate(single_step: Boolean, batch_interval: Long, config: PipelineConfig): Unit = {
    try {
      if (!single_step && batch_interval < 1) {
        throw new ApiException("batch_interval must be at least 1 second if single_step is not true")
      }

      // Assert that the phase configs, which are stored as JSON blobs, can be
      // deserialized properly.
      ExtractPhase.readConfig(config.extract.kind, config.extract.config)
      TransformPhase.readConfig(config.transform.kind, config.transform.config)
      LoadPhase.readConfig(config.load.kind, config.load.config)
    } catch {
      case e: DeserializationException => throw new ApiException(s"config does not validate: $e")
    }
  }
}
