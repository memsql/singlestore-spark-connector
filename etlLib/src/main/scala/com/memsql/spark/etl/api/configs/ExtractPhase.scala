package com.memsql.spark.etl.api.configs

import spray.json._
import ExtractPhaseKind._

object KafkaExtractOutputType extends Enumeration {
  type KafkaExtractOutputType = Value
  val String, ByteArray = Value
}
import KafkaExtractOutputType._

case class KafkaExtractConfig(kafka_brokers: String,
                              topics: List[String],
                              output_type: Option[KafkaExtractOutputType]) extends PhaseConfig

case class UserExtractConfig(value: String) extends PhaseConfig

object ExtractPhase extends JsonEnumProtocol {
  implicit val kafkaExtractOutputTypeFormat = jsonEnum(KafkaExtractOutputType)
  val kafkaConfigFormat = jsonFormat3(KafkaExtractConfig)
  val userConfigFormat = jsonFormat1(UserExtractConfig)

  def readConfig(kind: ExtractPhaseKind, config: JsValue): PhaseConfig = {
    kind match {
      case ExtractPhaseKind.User => userConfigFormat.read(config)
      case ExtractPhaseKind.Kafka => kafkaConfigFormat.read(config)
    }
  }

  def writeConfig(kind: ExtractPhaseKind, config: PhaseConfig): JsValue = {
    kind match {
      case ExtractPhaseKind.User => userConfigFormat.write(config.asInstanceOf[UserExtractConfig])
      case ExtractPhaseKind.Kafka => kafkaConfigFormat.write(config.asInstanceOf[KafkaExtractConfig])
    }
  }
}
