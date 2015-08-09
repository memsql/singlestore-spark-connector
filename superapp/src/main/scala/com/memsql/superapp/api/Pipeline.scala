package com.memsql.superapp.api

import com.memsql.spark.etl.api.configs.PipelineConfig
import spray.json._

object PipelineState extends Enumeration {
  type PipelineState = Value
  val RUNNING, STOPPED, ERROR = Value
}

import PipelineState._

case class Pipeline(pipeline_id: String,
                    var state: PipelineState,
                    jar: String,
                    var batch_interval: Long,
                    var config: PipelineConfig,
                    var error: Option[String] = None)
