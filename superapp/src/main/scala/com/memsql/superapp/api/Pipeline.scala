package com.memsql.superapp.api

import com.memsql.spark.etl.api.configs.PipelineConfig
import spray.json._

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
                    error: Option[String] = None)
