package com.memsql.superapp.api

import com.memsql.spark.etl.api.{PipelineConfig, PipelineExtractConfig}
import spray.json._

object PipelineState extends Enumeration {
  type PipelineState = Value
  val RUNNING, STOPPED, ERROR = Value
}

import PipelineState._

case class Pipeline(pipeline_id: String,
                    var state: PipelineState,
                    jar: String,
                    main_class: String,
                    var config: PipelineConfig,
                    var error: String = null)
