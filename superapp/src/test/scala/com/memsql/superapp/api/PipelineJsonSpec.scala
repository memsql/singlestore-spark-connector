package com.memsql.superapp.api

import com.memsql.spark.etl.api._
import com.memsql.superapp.api._
import com.memsql.superapp.api.ApiJsonProtocol._
import com.memsql.superapp.{TestKitSpec}
import ooyala.common.akka.web.JsonUtils._
import scala.util.{Success, Failure}
import spray.json._

class PipelineJsonSpec extends TestKitSpec("PipelineJsonSpec") {
  "Pipeline" should {
    "serialize to JSON" in {
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
      assert(jsonMap("config") == "{\"config_version\":42}")
      assert(jsonMap("error") == null)

      // Errors should be included.
      val pipeline2 = Pipeline(
        "pipeline1",
        state=PipelineState.RUNNING,
        jar="site.com/foo.jar",
        main_class="com.foo.FooMain",
        config=PipelineConfig(None, None, None),
        error="Test error")

      jsonString = pipeline2.toJson.toString
      jsonMap = mapFromJson(jsonString)
      assert(jsonMap("error") == "Test error")
    }

    "serialize to JSON with configs" in {
      val config = PipelineConfig(
        Some(PipelineExtractConfig(
          PipelineExtractType.KAFKA,
          PipelineKafkaExtractConfigData(
            "test1", "test2", Map("foo" -> 1)))),
        Some(PipelineTransformConfig(
          PipelineTransformType.USER,
          PipelineUserTransformConfigData("Test user data 1"))),
        Some(PipelineLoadConfig(
          PipelineLoadType.USER,
          PipelineUserLoadConfigData("Test user data 2"))))

      val pipeline = Pipeline(
        "pipeline1",
        state=PipelineState.RUNNING,
        jar="site.com/foo.jar",
        main_class="com.foo.FooMain",
        config=config)
      val jsonString = pipeline.toJson.toString
      val jsonMap = mapFromJson(jsonString)
      val configMap = mapFromJson(jsonMap("config").asInstanceOf[String])
      val extractConfigMap = configMap("extract_config").asInstanceOf[Map[String, Any]]
      assert(extractConfigMap("type") == "KAFKA")
      val kafkaConfigMap = mapFromJson(extractConfigMap("config").asInstanceOf[String])
      assert(kafkaConfigMap("zk_quorum") == "test1")
      assert(kafkaConfigMap("group_id") == "test2")
      assert(kafkaConfigMap("topics").asInstanceOf[Map[String, Any]]("foo") == 1)
      val transformConfigMap = configMap("transform_config").asInstanceOf[Map[String, Any]]
      assert(transformConfigMap("type") == "USER")
      val transformUserConfigMap = mapFromJson(transformConfigMap("config").asInstanceOf[String])
      assert(transformUserConfigMap("value") == "Test user data 1")
      val loadConfigMap = configMap("load_config").asInstanceOf[Map[String, Any]]
      assert(loadConfigMap("type") == "USER")
      val loadUserConfigMap = mapFromJson(loadConfigMap("config").asInstanceOf[String])
      assert(loadUserConfigMap("value") == "Test user data 2")
    }

    "deserialize from JSON" in {
      val jsonString = """{
        "pipeline_id": "pipeline1",
        "state": "RUNNING",
        "config": "{\"config_version\": 42, \"extract_config\":{\"type\":\"KAFKA\",\"config\":\"{\\\"zk_quorum\\\":\\\"test1\\\",\\\"group_id\\\":\\\"test2\\\",\\\"topics\\\":{\\\"foo\\\":1}}\"},\"transform_config\":{\"type\":\"USER\",\"config\":\"{\\\"value\\\":\\\"Test user data 1\\\"}\"},\"load_config\":{\"type\":\"USER\",\"config\":\"{\\\"value\\\":\\\"Test user data 2\\\"}\"}}",
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
      assert(pipeline.error == "test error")
      assert(pipeline.config.config_version == 42)
      assert(pipeline.config.extract_config.get.`type` == PipelineExtractType.KAFKA)
      assert(pipeline.config.extract_config.get.config.asInstanceOf[PipelineKafkaExtractConfigData].zk_quorum == "test1")
      assert(pipeline.config.extract_config.get.config.asInstanceOf[PipelineKafkaExtractConfigData].group_id == "test2")
      assert(pipeline.config.extract_config.get.config.asInstanceOf[PipelineKafkaExtractConfigData].topics("foo") == 1)
      assert(pipeline.config.transform_config.get.`type` == PipelineTransformType.USER)
      assert(pipeline.config.transform_config.get.config.asInstanceOf[PipelineUserTransformConfigData].value == "Test user data 1")
      assert(pipeline.config.load_config.get.`type` == PipelineLoadType.USER)
      assert(pipeline.config.load_config.get.config.asInstanceOf[PipelineUserLoadConfigData].value == "Test user data 2")
    }

    "be preserved through a round trip" in {
      val config = PipelineConfig(
        Some(PipelineExtractConfig(
          PipelineExtractType.KAFKA,
          PipelineKafkaExtractConfigData(
            "test1", "test2", Map("foo" -> 1)))),
        Some(PipelineTransformConfig(
          PipelineTransformType.USER,
          PipelineUserTransformConfigData("Test user data 1"))),
        Some(PipelineLoadConfig(
          PipelineLoadType.USER,
          PipelineUserLoadConfigData("Test user data 2"))),
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
}
