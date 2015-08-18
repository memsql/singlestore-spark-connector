package com.memsql.spark.interface.api

import com.memsql.spark.etl.api.configs._
import com.memsql.spark.interface._
import com.memsql.spark.interface.api.ApiJsonProtocol._
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
      state = PipelineState.RUNNING,
      jar = "site.com/foo.jar",
      batch_interval = 100,
      config = config,
      last_updated = 1)

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
      state = PipelineState.RUNNING,
      jar = "site.com/foo.jar",
      batch_interval = 12,
      config = config.copy(extract = Phase[ExtractPhaseKind](
        ExtractPhaseKind.TestJson,
        ExtractPhase.writeConfig(
          ExtractPhaseKind.TestJson,
          TestJsonExtractConfig(JsObject("test" -> JsString("bar")))
        )
      )),
      last_updated = 15,
      error = Some("Test error"))

    jsonString = pipeline2.toJson.toString
    jsonMap = mapFromJson(jsonString)
    assert(jsonMap("error") == "Test error")
    val configMap = jsonMap("config").asInstanceOf[Map[String, Any]]
    val extractMap = configMap("extract").asInstanceOf[Map[String, Any]]
    assert(extractMap("kind") == "TestJson")
    val extractConfigMap = extractMap("config").asInstanceOf[Map[String, Any]]
    assert(extractConfigMap("value").asInstanceOf[Map[String, Any]]("test") == "bar")
  }

  it should "serialize to JSON with configs" in {
    var config = PipelineConfig(
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

    var pipeline = Pipeline(
      "pipeline1",
      state=PipelineState.RUNNING,
      jar="site.com/foo.jar",
      batch_interval=100,
      last_updated=145,
      config=config)
    var jsonString = pipeline.toJson.toString
    var jsonMap = mapFromJson(jsonString)
    var configMap = jsonMap("config").asInstanceOf[Map[String, Any]]
    var extractConfigMap = configMap("extract").asInstanceOf[Map[String, Any]]
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

    config=config.copy(extract = Phase[ExtractPhaseKind](
      ExtractPhaseKind.TestString,
      ExtractPhase.writeConfig(
        ExtractPhaseKind.TestString,
        TestStringExtractConfig("test")
      )
    ))

    pipeline = Pipeline(
      "pipeline2",
      state=PipelineState.RUNNING,
      jar="site.com/foo.jar",
      batch_interval=100,
      last_updated=145,
      config=config)
    jsonString = pipeline.toJson.toString
    jsonMap = mapFromJson(jsonString)
    configMap = jsonMap("config").asInstanceOf[Map[String, Any]]
    extractConfigMap = configMap("extract").asInstanceOf[Map[String, Any]]
    assert(extractConfigMap("kind") == "TestString")
    val testJsonConfigMap = extractConfigMap("config").asInstanceOf[Map[String, Any]]
    assert(testJsonConfigMap("value") == "test")
  }

  it should "deserialize from JSON" in {
    var config_json = """{
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

    var jsonString = s"""{
        "pipeline_id": "pipeline1",
        "state": "RUNNING",
        "config": $config_json,
        "batch_interval": 100,
        "last_updated": 145,
        "error": "test error",
        "jar": "site.com/foo.jar",
        "active": true
      }"""
    var pipeline = jsonString.parseJson.convertTo[Pipeline]
    assert(pipeline.pipeline_id == "pipeline1")
    assert(pipeline.state == PipelineState.RUNNING)
    assert(pipeline.jar == "site.com/foo.jar")
    assert(pipeline.batch_interval == 100)
    assert(pipeline.last_updated == 145)
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

    config_json = """{
          "extract": {
              "kind": "TestJson",
              "config": {
                  "value": {
                      "nested": {
                          "values": [1,2,"43", false]
                      }
                  }
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

    jsonString = s"""{
        "pipeline_id": "pipeline1",
        "state": "RUNNING",
        "config": $config_json,
        "batch_interval": 100,
        "last_updated": 145,
        "error": "test error",
        "jar": "site.com/foo.jar",
        "active": true
      }"""
    pipeline = jsonString.parseJson.convertTo[Pipeline]
    assert(pipeline.pipeline_id == "pipeline1")
    assert(pipeline.state == PipelineState.RUNNING)
    assert(pipeline.jar == "site.com/foo.jar")
    assert(pipeline.batch_interval == 100)
    assert(pipeline.last_updated == 145)
    assert(pipeline.error == Some("test error"))
    assert(pipeline.config.config_version == 42)
    assert(pipeline.config.extract.kind == ExtractPhaseKind.TestJson)
    val jsonExtractConfig = ExtractPhase.readConfig(ExtractPhaseKind.TestJson, pipeline.config.extract.config).asInstanceOf[TestJsonExtractConfig]
    assert(jsonExtractConfig.value.asJsObject.fields("nested").asJsObject.fields("values").toString == "[1,2,\"43\",false]")
  }

  it should "be preserved through a round trip" in {
    var config = PipelineConfig(
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

    var pipeline1 = Pipeline(
      "pipeline1",
      state=PipelineState.RUNNING,
      jar="site.com/foo.jar",
      batch_interval=1234,
      last_updated=12,
      config=config)

    var pipeline2 = pipeline1.toJson.toString.parseJson.convertTo[Pipeline]
    assert(pipeline1 == pipeline2)

    config=config.copy(extract = Phase[ExtractPhaseKind](
      ExtractPhaseKind.TestJson,
      ExtractPhase.writeConfig(
        ExtractPhaseKind.TestJson,
        TestJsonExtractConfig(JsObject("test" -> JsString("bar")))
      )
    ))

    pipeline1 = Pipeline(
      "pipeline1",
      state=PipelineState.RUNNING,
      jar="site.com/foo.jar",
      batch_interval=1234,
      last_updated=12,
      config=config)

    pipeline2 = pipeline1.toJson.toString.parseJson.convertTo[Pipeline]
    assert(pipeline1 == pipeline2)
  }

  "PipelineMetricRecord" should "serialize to JSON" in {
    val metricRecord = PipelineMetricRecord(
      pipeline_id = "pipeline1",
      timestamp = 42,
      success = true,
      extract = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("extract error"),
        records = Some(List("extract record 1")),
        logs = Some(List("extract log 1"))
      )),
      transform = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("transform error"),
        records = Some(List("transform record 1")),
        logs = Some(List("transform log 1"))
      )),
      load = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("load error"),
        records = Some(List("load record 1")),
        logs = Some(List("load log 1"))
      ))
    )
    val jsonString = metricRecord.toJson.toString
    val jsonMap = mapFromJson(jsonString)
    val expectedMap = Map(
      "pipeline_id" -> "pipeline1",
      "timestamp" -> 42,
      "success" -> true,
      "extract" -> Map(
        "start" -> 1,
        "stop" -> 2,
        "count" -> 100,
        "error" -> "extract error",
        "records" -> List("extract record 1"),
        "logs" -> List("extract log 1")
      ),
      "transform" -> Map(
        "start" -> 1,
        "stop" -> 2,
        "count" -> 100,
        "error" -> "transform error",
        "records" -> List("transform record 1"),
        "logs" -> List("transform log 1")
      ),
      "load" -> Map(
        "start" -> 1,
        "stop" -> 2,
        "count" -> 100,
        "error" -> "load error",
        "records" -> List("load record 1"),
        "logs" -> List("load log 1")
      )
    )
    assert(jsonMap == expectedMap)
  }

  "PipelineMetricRecord" should "serialize with no phases" in {
    val metricRecord = PipelineMetricRecord(
      pipeline_id = "pipeline1",
      timestamp = 42,
      success = true,
      extract = None,
      transform = None,
      load = None
    )
    val jsonString = metricRecord.toJson.toString
    val jsonMap = mapFromJson(jsonString)
    val expectedMap = Map(
      "pipeline_id" -> "pipeline1",
      "timestamp" -> 42,
      "success" -> true
    )
    assert(jsonMap == expectedMap)
  }

  it should "deserialize from JSON" in {
    val metricRecord = PipelineMetricRecord(
      pipeline_id = "pipeline1",
      timestamp = 42,
      success = true,
      extract = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("extract error"),
        records = Some(List("extract record 1")),
        logs = Some(List("extract log 1"))
      )),
      transform = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("transform error"),
        records = Some(List("transform record 1")),
        logs = Some(List("transform log 1"))
      )),
      load = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("load error"),
        records = Some(List("load record 1")),
        logs = Some(List("load log 1"))
      ))
    )

    val jsonString = """{
        "pipeline_id": "pipeline1",
        "timestamp": 42,
        "success": true,
        "load": {
            "start": 1,
            "stop": 2,
            "count": 100,
            "error": "load error",
            "records": [
                "load record 1"
            ],
            "logs": [
                "load log 1"
            ]
        },
        "transform": {
            "start": 1,
            "stop": 2,
            "count": 100,
            "error": "transform error",
            "records": [
                "transform record 1"
            ],
            "logs": [
                "transform log 1"
            ]
        },
        "extract": {
            "start": 1,
            "stop": 2,
            "count": 100,
            "error": "extract error",
            "records": [
                "extract record 1"
            ],
            "logs": [
                "extract log 1"
            ]
        }
    }"""
    val parsedMetricRecord = jsonString.parseJson.convertTo[PipelineMetricRecord]
    assert(parsedMetricRecord == metricRecord)
  }

  it should "be preserved through a round trip" in {
    val metricRecord1 = PipelineMetricRecord(
      pipeline_id = "pipeline1",
      timestamp = 42,
      success = true,
      extract = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("extract error"),
        records = Some(List("extract record 1")),
        logs = Some(List("extract log 1"))
      )),
      transform = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("transform error"),
        records = Some(List("transform record 1")),
        logs = Some(List("transform log 1"))
      )),
      load = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("load error"),
        records = Some(List("load record 1")),
        logs = Some(List("load log 1"))
      ))
    )
    val metricRecord2 = metricRecord1.toJson.toString.parseJson.convertTo[PipelineMetricRecord]
    assert(metricRecord1 == metricRecord2)
  }
}
