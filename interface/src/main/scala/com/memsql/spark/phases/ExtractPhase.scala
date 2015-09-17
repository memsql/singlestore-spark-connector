package com.memsql.spark.phases

import com.memsql.spark.etl.api.configs.ExtractPhaseKind
import com.memsql.spark.etl.api.{UserExtractConfig, PhaseConfig}
import com.memsql.spark.etl.api.configs.ExtractPhaseKind._
import com.memsql.spark.etl.utils.JsonEnumProtocol
import spray.json._

case class KafkaExtractConfig(host: String, port: Int, topic: String) extends PhaseConfig

case class TestLinesExtractConfig(value: String) extends PhaseConfig

object ExtractPhase extends JsonEnumProtocol {
  val kafkaConfigFormat = jsonFormat3(KafkaExtractConfig)
  val testLinesConfigFormat = jsonFormat1(TestLinesExtractConfig)
  val userConfigFormat = jsonFormat2(UserExtractConfig)

  def readConfig(kind: ExtractPhaseKind, config: JsValue): PhaseConfig = {
    kind match {
      case ExtractPhaseKind.User => userConfigFormat.read(config)
      case ExtractPhaseKind.Kafka => kafkaConfigFormat.read(config)
      case ExtractPhaseKind.TestLines => testLinesConfigFormat.read(config)
    }
  }

  def writeConfig(kind: ExtractPhaseKind, config: PhaseConfig): JsValue = {
    kind match {
      case ExtractPhaseKind.User => userConfigFormat.write(config.asInstanceOf[UserExtractConfig])
      case ExtractPhaseKind.Kafka => kafkaConfigFormat.write(config.asInstanceOf[KafkaExtractConfig])
      case ExtractPhaseKind.TestLines => testLinesConfigFormat.write(config.asInstanceOf[TestLinesExtractConfig])
    }
  }
}
