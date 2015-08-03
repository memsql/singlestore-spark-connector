package com.memsql.spark.etl.api

object PipelineConfigVersion {
  val CurrentPipelineConfigVersion = 1
}
import PipelineConfigVersion._

// Config definitions for the extract phase
object PipelineExtractType extends Enumeration {
  type PipelineExtractType = Value
  val USER, KAFKA = Value
}
import PipelineExtractType._

abstract class PipelineExtractConfigData

case class PipelineKafkaExtractConfigData(zk_quorum: String,
                                          group_id: String,
                                          topics: Map[String, Int])
    extends PipelineExtractConfigData

case class PipelineUserExtractConfigData(value: String)
    extends PipelineExtractConfigData

case class PipelineExtractConfig(`type`: PipelineExtractType,
                                 config: PipelineExtractConfigData)

// Config definitions for the transform phase
object PipelineTransformType extends Enumeration {
  type PipelineTransformType = Value
  val USER = Value
}
import PipelineTransformType._

abstract class PipelineTransformConfigData

case class PipelineUserTransformConfigData(value: String)
    extends PipelineTransformConfigData

case class PipelineTransformConfig(`type`: PipelineTransformType,
                                   config: PipelineTransformConfigData)

// Config definitions for the load phase
object PipelineLoadType extends Enumeration {
  type PipelineLoadType = Value
  val USER, MEMSQL = Value
}
import PipelineLoadType._

abstract class PipelineLoadConfigData

case class PipelineMemSQLLoadConfigData(db_name: String, table_name: String)
    extends PipelineLoadConfigData

case class PipelineUserLoadConfigData(value: String)
    extends PipelineLoadConfigData

case class PipelineLoadConfig(`type`: PipelineLoadType,
                              config: PipelineLoadConfigData)

// The PipelineConfig object contains configuration for all stages of an
// ETL pipeline.
case class PipelineConfig(var extract_config: Option[PipelineExtractConfig],
                          var transform_config: Option[PipelineTransformConfig],
                          var load_config: Option[PipelineLoadConfig],
                          var config_version: Int = PipelineConfigVersion.CurrentPipelineConfigVersion)
