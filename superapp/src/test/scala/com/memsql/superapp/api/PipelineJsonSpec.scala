package com.memsql.superapp.api

import com.memsql.spark.etl.api.configs._
import com.memsql.superapp.api._
import com.memsql.superapp.api.ApiJsonProtocol._
import com.memsql.superapp.UnitSpec
import ooyala.common.akka.web.JsonUtils._
import scala.util.{Success, Failure}
import spray.json._

import ExtractPhaseKind._
import TransformPhaseKind._
import LoadPhaseKind._

class PipelineJsonSpec extends UnitSpec {
  "Pipeline" should "serialize to JSON" in {
    val pipeline1 = Pipeline(
      "pipeline1",
      state=PipelineState.RUNNING,
      jar="site.com/foo.jar",
      main_class="com.foo.FooMain",
      config=PipelineConfig(None, None, None, config_version=42))

    var jsonString = pipeline1.toJson.toString
    var jsonMap = mapFromJson(jsonString)
    assert(jsonMap("pipeline_id") == "pipeline1")
    assert(jsonMap("state") == "RUNNING")
    assert(jsonMap("jar") == "site.com/foo.jar")
    assert(jsonMap("main_class") == "com.foo.FooMain")
    assert(jsonMap("config") == Map("config_version" -> 42))
    assert(!(jsonMap contains "error"))

    // Errors should be included.
    val pipeline2 = Pipeline(
      "pipeline1",
      state=PipelineState.RUNNING,
      jar="site.com/foo.jar",
      main_class="com.foo.FooMain",
      config=PipelineConfig(None, None, None),
      error=Some("Test error"))

    jsonString = pipeline2.toJson.toString
    jsonMap = mapFromJson(jsonString)
    assert(jsonMap("error") == "Test error")
  }

  it should "serialize to JSON with configs" in {
    val config = PipelineConfig(
      Some(Phase[ExtractPhaseKind](
        ExtractPhaseKind.Kafka,
        ExtractPhase.writeConfig(
          ExtractPhaseKind.Kafka, KafkaExtractConfig("test1", List("test2"), None)))),
      Some(Phase[TransformPhaseKind](
        TransformPhaseKind.User,
        TransformPhase.writeConfig(
          TransformPhaseKind.User, UserTransformConfig("Test user data 1")))),
      Some(Phase[LoadPhaseKind](
        LoadPhaseKind.User,
        LoadPhase.writeConfig(
          LoadPhaseKind.User, UserLoadConfig("Test user data 2")))),
      config_version=42)

    val pipeline = Pipeline(
      "pipeline1",
      state=PipelineState.RUNNING,
      jar="site.com/foo.jar",
      main_class="com.foo.FooMain",
      config=config)
    val jsonString = pipeline.toJson.toString
    val jsonMap = mapFromJson(jsonString)
    val configMap = jsonMap("config").asInstanceOf[Map[String, Any]]
    val extractConfigMap = configMap("extract").asInstanceOf[Map[String, Any]]
    assert(extractConfigMap("kind") == "Kafka")
    val kafkaConfigMap = extractConfigMap("config").asInstanceOf[Map[String, Any]]
    assert(kafkaConfigMap("kafka_brokers") == "test1")
    assert(kafkaConfigMap("topics") == List("test2"))
    assert(!(kafkaConfigMap contains "output_type"))
    val transformConfigMap = configMap("transform").asInstanceOf[Map[String, Any]]
    assert(transformConfigMap("kind") == "User")
    val transformUserConfigMap = transformConfigMap("config").asInstanceOf[Map[String, Any]]
    assert(transformUserConfigMap("value") == "Test user data 1")
    val loadConfigMap = configMap("load").asInstanceOf[Map[String, Any]]
    assert(loadConfigMap("kind") == "User")
    val loadUserConfigMap = loadConfigMap("config").asInstanceOf[Map[String, Any]]
    assert(loadUserConfigMap("value") == "Test user data 2")
  }

  it should "deserialize from JSON" in {
    val config_json = """{
          "extract": {
              "kind": "Kafka",
              "config": {
                  "kafka_brokers": "test1",
                  "topics": [ "test2" ]
              }
          },
          "transform": {
              "kind": "User",
              "config": {
                  "value": "Test user data 1"
              }
          },
          "load": {
              "kind": "User",
              "config": {
                  "value": "Test user data 2"
              }
          },
          "config_version": 42
      }
      """

    val jsonString = s"""{
        "pipeline_id": "pipeline1",
        "state": "RUNNING",
        "config": $config_json,
        "main_class": "com.foo.FooMain",
        "error": "test error",
        "jar": "site.com/foo.jar",
        "active": true
      }"""
    val pipeline = jsonString.parseJson.convertTo[Pipeline]
    assert(pipeline.pipeline_id == "pipeline1")
    assert(pipeline.state == PipelineState.RUNNING)
    assert(pipeline.jar == "site.com/foo.jar")
    assert(pipeline.main_class == "com.foo.FooMain")
    assert(pipeline.error == Some("test error"))
    assert(pipeline.config.config_version == 42)
    assert(pipeline.config.extract.get.kind == ExtractPhaseKind.Kafka)
    val kafkaConfig = ExtractPhase.readConfig(pipeline.config.extract.get.kind, pipeline.config.extract.get.config).asInstanceOf[KafkaExtractConfig]
    assert(kafkaConfig.kafka_brokers == "test1")
    assert(kafkaConfig.topics == List("test2"))
    assert(pipeline.config.transform.get.kind == TransformPhaseKind.User)
    val userTransformConfig = TransformPhase.readConfig(pipeline.config.transform.get.kind, pipeline.config.transform.get.config).asInstanceOf[UserTransformConfig]
    assert(userTransformConfig.value == "Test user data 1")
    assert(pipeline.config.load.get.kind == LoadPhaseKind.User)
    val userLoadConfig = LoadPhase.readConfig(pipeline.config.load.get.kind, pipeline.config.load.get.config).asInstanceOf[UserLoadConfig]
    assert(userLoadConfig.value == "Test user data 2")
  }

  it should "be preserved through a round trip" in {
    val config = PipelineConfig(
      Some(Phase[ExtractPhaseKind](
        ExtractPhaseKind.Kafka,
        ExtractPhase.writeConfig(
          ExtractPhaseKind.Kafka, KafkaExtractConfig("test1", List("test2"), None)))),
      Some(Phase[TransformPhaseKind](
        TransformPhaseKind.User,
        TransformPhase.writeConfig(
          TransformPhaseKind.User, UserTransformConfig("Test user data 1")))),
      Some(Phase[LoadPhaseKind](
        LoadPhaseKind.User,
        LoadPhase.writeConfig(
          LoadPhaseKind.User, UserLoadConfig("Test user data 2")))),
      config_version=42)

    val pipeline1 = Pipeline(
      "pipeline1",
      state=PipelineState.RUNNING,
      jar="site.com/foo.jar",
      main_class="com.foo.FooMain",
      config=config)

    val pipeline2 = pipeline1.toJson.toString.parseJson.convertTo[Pipeline]
    assert(pipeline1 == pipeline2)
  }
}
