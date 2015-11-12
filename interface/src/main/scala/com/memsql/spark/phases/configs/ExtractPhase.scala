package com.memsql.spark.phases.configs

import com.memsql.spark.etl.api.configs.ExtractPhaseKind
import com.memsql.spark.etl.api.configs.ExtractPhaseKind._
import com.memsql.spark.etl.api.{PhaseConfig, UserExtractConfig}
import com.memsql.spark.etl.utils.JsonEnumProtocol
import spray.json._

import com.memsql.spark.phases.{PythonExtractConfig, ZookeeperManagedKafkaExtractConfig, TestLinesExtractConfig, KafkaExtractConfig}

object ExtractPhase extends JsonEnumProtocol {
  val kafkaConfigFormat = jsonFormat3(KafkaExtractConfig)
  val zookeeperManagedKafkaConfigFormat = jsonFormat2(ZookeeperManagedKafkaExtractConfig)
  val testLinesConfigFormat = jsonFormat1(TestLinesExtractConfig)
  val userConfigFormat = jsonFormat2(UserExtractConfig)
  val pythonConfigFormat = jsonFormat2(PythonExtractConfig)

  def readConfig(kind: ExtractPhaseKind, config: JsValue): PhaseConfig = {
    kind match {
      case ExtractPhaseKind.User => userConfigFormat.read(config)
      case ExtractPhaseKind.ZookeeperManagedKafka => zookeeperManagedKafkaConfigFormat.read(config)
      case ExtractPhaseKind.Kafka => kafkaConfigFormat.read(config)
      case ExtractPhaseKind.TestLines => testLinesConfigFormat.read(config)
      case ExtractPhaseKind.Python => pythonConfigFormat.read(config)
    }
  }

  def writeConfig(kind: ExtractPhaseKind, config: PhaseConfig): JsValue = {
    kind match {
      case ExtractPhaseKind.User => userConfigFormat.write(config.asInstanceOf[UserExtractConfig])
      case ExtractPhaseKind.ZookeeperManagedKafka => zookeeperManagedKafkaConfigFormat.write(config.asInstanceOf[ZookeeperManagedKafkaExtractConfig])
      case ExtractPhaseKind.Kafka => kafkaConfigFormat.write(config.asInstanceOf[KafkaExtractConfig])
      case ExtractPhaseKind.TestLines => testLinesConfigFormat.write(config.asInstanceOf[TestLinesExtractConfig])
      case ExtractPhaseKind.Python => pythonConfigFormat.write(config.asInstanceOf[PythonExtractConfig])
    }
  }
}
