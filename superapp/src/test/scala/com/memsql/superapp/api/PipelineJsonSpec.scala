package com.memsql.superapp.api

import com.memsql.spark.etl.api.configs._
import com.memsql.superapp.api.ApiJsonProtocol._
import com.memsql.superapp.UnitSpec
import ooyala.common.akka.web.JsonUtils._
import spray.json._

import ExtractPhaseKind._
import TransformPhaseKind._
import LoadPhaseKind._

class PipelineJsonSpec extends UnitSpec {
  "Pipeline" should "serialize to JSON" in {
    val config = PipelineConfig(
      Phase[ExtractPhaseKind](
        ExtractPhaseKind.User,
        ExtractPhase.writeConfig(
          ExtractPhaseKind.User, UserExtractConfig("com.test.ExtractClass", ""))),
      Phase[TransformPhaseKind](
        TransformPhaseKind.User,
        TransformPhase.writeConfig(
          TransformPhaseKind.User, UserTransformConfig("com.test.TransformClass", "test1"))),
      Phase[LoadPhaseKind](
        LoadPhaseKind.MemSQL,
        LoadPhase.writeConfig(
          LoadPhaseKind.MemSQL, MemSQLLoadConfig("db", "table", None, None, None))),
      config_version = 42)

    val pipeline1 = Pipeline(
      "pipeline1",
      state=PipelineState.RUNNING,
      jar="site.com/foo.jar",
      batch_interval=100,
      config=config)

    var jsonString = pipeline1.toJson.toString
    var jsonMap = mapFromJson(jsonString)
    assert(jsonMap("pipeline_id") == "pipeline1")
    assert(jsonMap("state") == "RUNNING")
    assert(jsonMap("jar") == "site.com/foo.jar")
    assert(jsonMap("batch_interval") == 100)
    assert(jsonMap("config").asInstanceOf[Map[String, Any]]("config_version") == 42)
    assert(!(jsonMap contains "error"))

    // Errors should be included.
    val pipeline2 = Pipeline(
      "pipeline1",
      state=PipelineState.RUNNING,
      jar="site.com/foo.jar",
      batch_interval=12,
      config=config,
      error=Some("Test error"))

    jsonString = pipeline2.toJson.toString
    jsonMap = mapFromJson(jsonString)
    assert(jsonMap("error") == "Test error")
  }

  it should "serialize to JSON with configs" in {
    val config = PipelineConfig(
      Phase[ExtractPhaseKind](
        ExtractPhaseKind.Kafka,
        ExtractPhase.writeConfig(
          ExtractPhaseKind.Kafka, KafkaExtractConfig("test1", 9092, "test2", None))),
      Phase[TransformPhaseKind](
        TransformPhaseKind.User,
        TransformPhase.writeConfig(
          TransformPhaseKind.User, UserTransformConfig("com.user.Transform", "Test user data 1"))),
      Phase[LoadPhaseKind](
        LoadPhaseKind.User,
        LoadPhase.writeConfig(
          LoadPhaseKind.User, UserLoadConfig("com.user.Load", "Test user data 2"))),
      config_version=42)

    val pipeline = Pipeline(
      "pipeline1",
      state=PipelineState.RUNNING,
      jar="site.com/foo.jar",
      batch_interval=100,
      config=config)
    val jsonString = pipeline.toJson.toString
    val jsonMap = mapFromJson(jsonString)
    val configMap = jsonMap("config").asInstanceOf[Map[String, Any]]
    val extractConfigMap = configMap("extract").asInstanceOf[Map[String, Any]]
    assert(extractConfigMap("kind") == "Kafka")
    val kafkaConfigMap = extractConfigMap("config").asInstanceOf[Map[String, Any]]
    assert(kafkaConfigMap("host") == "test1")
    assert(kafkaConfigMap("port") == 9092)
    assert(kafkaConfigMap("topic") == "test2")
    assert(!(kafkaConfigMap contains "output_type"))
    val transformConfigMap = configMap("transform").asInstanceOf[Map[String, Any]]
    assert(transformConfigMap("kind") == "User")
    val transformUserConfigMap = transformConfigMap("config").asInstanceOf[Map[String, Any]]
    assert(transformUserConfigMap("class_name") == "com.user.Transform")
    assert(transformUserConfigMap("value") == "Test user data 1")
    val loadConfigMap = configMap("load").asInstanceOf[Map[String, Any]]
    assert(loadConfigMap("kind") == "User")
    val loadUserConfigMap = loadConfigMap("config").asInstanceOf[Map[String, Any]]
    assert(loadUserConfigMap("class_name") == "com.user.Load")
    assert(loadUserConfigMap("value") == "Test user data 2")
  }

  it should "deserialize from JSON" in {
    val config_json = """{
          "extract": {
              "kind": "Kafka",
              "config": {
                  "host": "test1",
                  "port": 9091,
                  "topic": "test2"
              }
          },
          "transform": {
              "kind": "User",
              "config": {
                  "class_name": "com.user.Transform",
                  "value": "Test user data 1"
              }
          },
          "load": {
              "kind": "User",
              "config": {
                  "class_name": "com.user.Load",
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
        "batch_interval": 100,
        "error": "test error",
        "jar": "site.com/foo.jar",
        "active": true
      }"""
    val pipeline = jsonString.parseJson.convertTo[Pipeline]
    assert(pipeline.pipeline_id == "pipeline1")
    assert(pipeline.state == PipelineState.RUNNING)
    assert(pipeline.jar == "site.com/foo.jar")
    assert(pipeline.batch_interval == 100)
    assert(pipeline.error == Some("test error"))
    assert(pipeline.config.config_version == 42)
    assert(pipeline.config.extract.kind == ExtractPhaseKind.Kafka)
    val kafkaConfig = ExtractPhase.readConfig(pipeline.config.extract.kind, pipeline.config.extract.config).asInstanceOf[KafkaExtractConfig]
    assert(kafkaConfig.host == "test1")
    assert(kafkaConfig.port == 9091)
    assert(kafkaConfig.topic == "test2")
    assert(pipeline.config.transform.kind == TransformPhaseKind.User)
    val userTransformConfig = TransformPhase.readConfig(pipeline.config.transform.kind, pipeline.config.transform.config).asInstanceOf[UserTransformConfig]
    assert(userTransformConfig.class_name == "com.user.Transform")
    assert(userTransformConfig.value == "Test user data 1")
    assert(pipeline.config.load.kind == LoadPhaseKind.User)
    val userLoadConfig = LoadPhase.readConfig(pipeline.config.load.kind, pipeline.config.load.config).asInstanceOf[UserLoadConfig]
    assert(userLoadConfig.class_name == "com.user.Load")
    assert(userLoadConfig.value == "Test user data 2")
  }

  it should "be preserved through a round trip" in {
    val config = PipelineConfig(
      Phase[ExtractPhaseKind](
        ExtractPhaseKind.Kafka,
        ExtractPhase.writeConfig(
          ExtractPhaseKind.Kafka, KafkaExtractConfig("test1", 9090, "test2", None))),
      Phase[TransformPhaseKind](
        TransformPhaseKind.User,
        TransformPhase.writeConfig(
          TransformPhaseKind.User, UserTransformConfig("com.user.Transform", "Test user data 1"))),
      Phase[LoadPhaseKind](
        LoadPhaseKind.User,
        LoadPhase.writeConfig(
          LoadPhaseKind.User, UserLoadConfig("com.user.Load", "Test user data 2"))),
      config_version=42)

    val pipeline1 = Pipeline(
      "pipeline1",
      state=PipelineState.RUNNING,
      jar="site.com/foo.jar",
      batch_interval=1234,
      config=config)

    val pipeline2 = pipeline1.toJson.toString.parseJson.convertTo[Pipeline]
    assert(pipeline1 == pipeline2)
  }
}
