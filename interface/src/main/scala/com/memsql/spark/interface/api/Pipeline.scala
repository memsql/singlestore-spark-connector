package com.memsql.spark.interface.api

import com.memsql.spark.etl.api.configs._
import com.memsql.spark.interface.util.BoundedQueue
import spray.json.DeserializationException

object PipelineState extends Enumeration {
  type PipelineState = Value
  val RUNNING, STOPPED, ERROR = Value
}

import PipelineState._

case class Pipeline(pipeline_id: String,
                    state: PipelineState,
                    batch_interval: Long,
                    config: PipelineConfig,
                    last_updated: Long,
                    error: Option[String] = None) {
  Pipeline.validate(batch_interval, config)

  val MAX_METRICS_QUEUE_SIZE = 1000
  @volatile private[interface] var metricsQueue = new BoundedQueue[PipelineMetricRecord](MAX_METRICS_QUEUE_SIZE)
  @volatile private[interface] var traceBatchCount = 0

  private[interface] def enqueueMetricRecord(records: PipelineMetricRecord*) = {
    metricsQueue.enqueue(records: _*)
  }
}

object Pipeline {
  def validate(batch_interval: Long, config: PipelineConfig): Unit = {
    try {
      if (batch_interval <= 0) {
        throw new ApiException("batch_interval must be positive")
      }

      // Assert that the phase configs, which are stored as JSON blobs, can be
      // deserialized properly.
      ExtractPhase.readConfig(config.extract.kind, config.extract.config)
      TransformPhase.readConfig(config.transform.kind, config.transform.config)
      LoadPhase.readConfig(config.load.kind, config.load.config)

      val requiresJar = config.extract.kind == ExtractPhaseKind.User ||
        config.transform.kind == TransformPhaseKind.User ||
        config.load.kind == LoadPhaseKind.User

      if (requiresJar && config.jar.isEmpty) {
        throw new ApiException("jar is required for user defined phases")
      }
    } catch {
      case e: DeserializationException => throw new ApiException(s"config does not validate: $e")
    }
  }
}
