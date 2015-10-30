package com.memsql.spark.interface.api

import java.util.UUID

import com.memsql.spark.etl.api.{UserTransformConfig, UserExtractConfig}
import com.memsql.spark.etl.api.configs._
import com.memsql.spark.interface._
import com.memsql.spark.interface.api.ApiJsonProtocol._
import com.memsql.spark.phases.{ZookeeperManagedKafkaExtractConfig, JsonTransformConfig, TestLinesExtractConfig}
import com.memsql.spark.phases.configs.ExtractPhase
import ooyala.common.akka.web.JsonUtils._
import spray.json._

import ExtractPhaseKind._
import TransformPhaseKind._
import LoadPhaseKind._

// scalastyle:off magic.number
class PipelineJsonSpec extends UnitSpec {
  "Pipeline" should "serialize to JSON" in {
    val config = PipelineConfig(
      Phase[ExtractPhaseKind](
        ExtractPhaseKind.User,
        ExtractPhase.writeConfig(
          ExtractPhaseKind.User, UserExtractConfig("com.test.ExtractClass", JsNull))),
      Phase[TransformPhaseKind](
        TransformPhaseKind.User,
        TransformPhase.writeConfig(
          TransformPhaseKind.User, UserTransformConfig("com.test.TransformClass", JsNull))),
      Phase[LoadPhaseKind](
        LoadPhaseKind.MemSQL,
        LoadPhase.writeConfig(
          LoadPhaseKind.MemSQL, MemSQLLoadConfig("db", "table", None, None))),
      config_version = 42)

    val pipeline1 = Pipeline(
      "pipeline1",
      state = PipelineState.RUNNING,
      batch_interval = 100,
      config = config,
      last_updated = 1)

    var jsonString = pipeline1.toJson.toString
    var jsonMap = mapFromJson(jsonString)
    assert(jsonMap("pipeline_id") == "pipeline1")
    assert(jsonMap("state") == "RUNNING")
    assert(jsonMap("batch_interval") == 100)
    assert(jsonMap("config").asInstanceOf[Map[String, Any]]("config_version") == 42)
    assert(!jsonMap("config").asInstanceOf[Map[String, Any]].contains("enable_checkpointing"))
    assert(!(jsonMap contains "error"))

    // Errors should be included.
    val pipeline2 = Pipeline(
      "pipeline1",
      state = PipelineState.RUNNING,
      batch_interval = 12,
      config = config.copy(extract = Phase[ExtractPhaseKind](
        ExtractPhaseKind.TestLines,
        ExtractPhase.writeConfig(
          ExtractPhaseKind.TestLines,
          TestLinesExtractConfig("test")
        )),
        transform = Phase[TransformPhaseKind](
          TransformPhaseKind.Json,
          TransformPhase.writeConfig(TransformPhaseKind.Json, JsonTransformConfig("data"))
        ),
        enable_checkpointing = Some(true)
      ),
      last_updated = 15,
      error = Some("Test error"))

    jsonString = pipeline2.toJson.toString
    jsonMap = mapFromJson(jsonString)
    assert(jsonMap("error") == "Test error")
    val configMap = jsonMap("config").asInstanceOf[Map[String, Any]]
    assert(configMap("enable_checkpointing") == true)
    val extractMap = configMap("extract").asInstanceOf[Map[String, Any]]
    assert(extractMap("kind") == "TestLines")
    val extractConfigMap = extractMap("config").asInstanceOf[Map[String, Any]]
    assert(extractConfigMap("value").asInstanceOf[String] == "test")
    val transformMap = configMap("transform").asInstanceOf[Map[String, Any]]
    assert(transformMap("kind") == "Json")
    val transformConfigMap = transformMap("config").asInstanceOf[Map[String, Any]]
    assert(transformConfigMap("column_name") == "data")
  }

  it should "serialize to JSON with configs" in {
    var config = PipelineConfig(
      Phase[ExtractPhaseKind](
        ExtractPhaseKind.ZookeeperManagedKafka,
        ExtractPhase.writeConfig(
          ExtractPhaseKind.ZookeeperManagedKafka, ZookeeperManagedKafkaExtractConfig(List("test1:2181"), "test2"))),
      Phase[TransformPhaseKind](
        TransformPhaseKind.User,
        TransformPhase.writeConfig(
          TransformPhaseKind.User, UserTransformConfig("com.user.Transform", JsString("Test user data 1")))),
      Phase[LoadPhaseKind](
        LoadPhaseKind.MemSQL,
        LoadPhase.writeConfig(
          LoadPhaseKind.MemSQL, MemSQLLoadConfig("db", "table", None, None))),
      config_version=42)

    var pipeline = Pipeline(
      "pipeline1",
      state=PipelineState.RUNNING,
      batch_interval=100,
      last_updated=145,
      config=config)
    var jsonString = pipeline.toJson.toString
    var jsonMap = mapFromJson(jsonString)
    var configMap = jsonMap("config").asInstanceOf[Map[String, Any]]
    var extractConfigMap = configMap("extract").asInstanceOf[Map[String, Any]]
    assert(extractConfigMap("kind") == "ZookeeperManagedKafka")
    val kafkaConfigMap = extractConfigMap("config").asInstanceOf[Map[String, Any]]
    assert(kafkaConfigMap("zk_quorum") == List("test1:2181"))
    assert(kafkaConfigMap("topic") == "test2")
    val transformConfigMap = configMap("transform").asInstanceOf[Map[String, Any]]
    assert(transformConfigMap("kind") == "User")
    val transformUserConfigMap = transformConfigMap("config").asInstanceOf[Map[String, Any]]
    assert(transformUserConfigMap("class_name") == "com.user.Transform")
    assert(transformUserConfigMap("value") == "Test user data 1")
    val loadConfigMap = configMap("load").asInstanceOf[Map[String, Any]]
    assert(loadConfigMap("kind") == "MemSQL")
    val loadUserConfigMap = loadConfigMap("config").asInstanceOf[Map[String, Any]]
    assert(loadUserConfigMap("db_name") == "db")
    assert(loadUserConfigMap("table_name") == "table")
    assert(loadUserConfigMap("dry_run") == false)

    config=config.copy(extract = Phase[ExtractPhaseKind](
      ExtractPhaseKind.TestLines,
      ExtractPhase.writeConfig(
        ExtractPhaseKind.TestLines,
        TestLinesExtractConfig("test")
      )
    ))

    pipeline = Pipeline(
      "pipeline2",
      state=PipelineState.RUNNING,
      batch_interval=100,
      last_updated=145,
      config=config)
    jsonString = pipeline.toJson.toString
    jsonMap = mapFromJson(jsonString)
    configMap = jsonMap("config").asInstanceOf[Map[String, Any]]
    extractConfigMap = configMap("extract").asInstanceOf[Map[String, Any]]
    assert(extractConfigMap("kind") == "TestLines")
    val testLinesConfigMap = extractConfigMap("config").asInstanceOf[Map[String, Any]]
    assert(testLinesConfigMap("value") == "test")
  }

  it should "deserialize from JSON" in {
    var config_json = """{
          "extract": {
              "kind": "ZookeeperManagedKafka",
              "config": {
                  "zk_quorum": ["test2:2181", "asdf:1000/asdf"],
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
              "kind": "MemSQL",
              "config": {
                  "db_name": "db",
                  "table_name": "table",
                  "dry_run": true
              }
          },
          "config_version": 42,
          "enable_checkpointing": true
      }
      """

    var jsonString = s"""{
        "pipeline_id": "pipeline1",
        "state": "RUNNING",
        "config": $config_json,
        "batch_interval": 100,
        "last_updated": 145,
        "error": "test error",
        "active": true,
        "thread_state": "THREAD_RUNNING"
      }"""
    var pipeline = jsonString.parseJson.convertTo[Pipeline]
    assert(pipeline.pipeline_id == "pipeline1")
    assert(pipeline.state == PipelineState.RUNNING)
    assert(pipeline.batch_interval == 100)
    assert(pipeline.last_updated == 145)
    assert(pipeline.error.get == "test error")
    assert(pipeline.config.config_version == 42)
    assert(pipeline.config.enable_checkpointing.get == true)
    assert(pipeline.config.extract.kind == ExtractPhaseKind.ZookeeperManagedKafka)
    val kafkaConfig = ExtractPhase.readConfig(pipeline.config.extract.kind, pipeline.config.extract.config).asInstanceOf[ZookeeperManagedKafkaExtractConfig]
    assert(kafkaConfig.zk_quorum == List("test2:2181", "asdf:1000/asdf"))
    assert(kafkaConfig.topic == "test2")
    assert(pipeline.config.transform.kind == TransformPhaseKind.User)
    val userTransformConfig = TransformPhase.readConfig(pipeline.config.transform.kind, pipeline.config.transform.config).asInstanceOf[UserTransformConfig]
    assert(userTransformConfig.class_name == "com.user.Transform")
    assert(userTransformConfig.value.toString == "\"Test user data 1\"")
    assert(pipeline.config.load.kind == LoadPhaseKind.MemSQL)
    val memsqlLoadConfig = LoadPhase.readConfig(pipeline.config.load.kind, pipeline.config.load.config).asInstanceOf[MemSQLLoadConfig]
    assert(memsqlLoadConfig.db_name == "db")
    assert(memsqlLoadConfig.table_name == "table")
    assert(memsqlLoadConfig.dry_run)

    config_json = """{
          "extract": {
              "kind": "TestLines",
              "config": {
                  "value": "test"
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
              "kind": "MemSQL",
              "config": {
                  "db_name": "db",
                  "table_name": "table",
                  "dry_run": false
              }
          },
          "config_version": 42,
          "enable_checkpointing": false
      }"""

    jsonString = s"""{
        "pipeline_id": "pipeline1",
        "state": "RUNNING",
        "config": $config_json,
        "batch_interval": 100,
        "last_updated": 145,
        "error": "test error",
        "active": true,
        "thread_state": "THREAD_RUNNING"
      }"""
    pipeline = jsonString.parseJson.convertTo[Pipeline]
    assert(pipeline.pipeline_id == "pipeline1")
    assert(pipeline.state == PipelineState.RUNNING)
    assert(pipeline.batch_interval == 100)
    assert(pipeline.last_updated == 145)
    assert(pipeline.error.get  == "test error")
    assert(pipeline.config.config_version == 42)
    assert(pipeline.config.enable_checkpointing.get == false)
    assert(pipeline.config.extract.kind == ExtractPhaseKind.TestLines)
    val testLinesConfig = ExtractPhase.readConfig(ExtractPhaseKind.TestLines, pipeline.config.extract.config).asInstanceOf[TestLinesExtractConfig]
    assert(testLinesConfig.value == "test")
  }

  it should "be preserved through a round trip" in {
    var config = PipelineConfig(
      Phase[ExtractPhaseKind](
        ExtractPhaseKind.ZookeeperManagedKafka,
        ExtractPhase.writeConfig(
          ExtractPhaseKind.ZookeeperManagedKafka, ZookeeperManagedKafkaExtractConfig(List("test1:2181"), "test2"))),
      Phase[TransformPhaseKind](
        TransformPhaseKind.User,
        TransformPhase.writeConfig(
          TransformPhaseKind.User, UserTransformConfig("com.user.Transform", JsString("Test user data 1")))),
      Phase[LoadPhaseKind](
        LoadPhaseKind.MemSQL,
        LoadPhase.writeConfig(
          LoadPhaseKind.MemSQL, MemSQLLoadConfig("db", "table", None, None))),
      config_version=42)

    var pipeline1 = Pipeline(
      "pipeline1",
      state=PipelineState.RUNNING,
      batch_interval=1234,
      last_updated=12,
      config=config)

    var pipeline2 = pipeline1.toJson.toString.parseJson.convertTo[Pipeline]
    assert(pipeline1 == pipeline2)

    config=config.copy(extract = Phase[ExtractPhaseKind](
      ExtractPhaseKind.TestLines,
      ExtractPhase.writeConfig(
        ExtractPhaseKind.TestLines,
        TestLinesExtractConfig("test")
      )
    ))

    pipeline1 = Pipeline(
      "pipeline1",
      state=PipelineState.RUNNING,
      batch_interval=1234,
      last_updated=12,
      config=config)

    pipeline2 = pipeline1.toJson.toString.parseJson.convertTo[Pipeline]
    assert(pipeline1 == pipeline2)
  }

  "PipelineStartEvent" should "serialize to JSON" in {
    val uuid = UUID.randomUUID.toString
    val metricRecord = PipelineStartEvent(
      pipeline_id = "pipeline1",
      timestamp = 42,
      event_id = uuid
    )
    val jsonString = metricRecord.toJson.toString
    val jsonMap = mapFromJson(jsonString)
    val expectedMap = Map(
      "pipeline_id" -> "pipeline1",
      "timestamp" -> 42,
      "event_type" -> "PipelineStart",
      "event_id" -> uuid
    )
    assert(jsonMap == expectedMap)
  }

  "PipelineStartEvent" should "serialize with no phases" in {
    val uuid = UUID.randomUUID.toString
    val metricRecord = PipelineStartEvent(
      pipeline_id = "pipeline1",
      timestamp = 42,
      event_id = uuid
    )
    val jsonString = metricRecord.toJson.toString
    val jsonMap = mapFromJson(jsonString)
    val expectedMap = Map(
      "pipeline_id" -> "pipeline1",
      "timestamp" -> 42,
      "event_type" -> "PipelineStart",
      "event_id" -> uuid
    )
    assert(jsonMap == expectedMap)
  }

  "PipelineStartEvent" should "deserialize from JSON" in {
    val uuid = UUID.randomUUID.toString
    val metricRecord = PipelineStartEvent(
      pipeline_id = "pipeline1",
      timestamp = 42,
      event_id = uuid
    )
    val jsonString = s"""{
        "pipeline_id": "pipeline1",
        "timestamp": 42,
        "event_type": "PipelineStart",
        "event_id": "${uuid}"
    }"""
    val parsedMetricRecord = jsonString.parseJson.convertTo[PipelineStartEvent]
    assert(parsedMetricRecord == metricRecord)
  }


  "PipelineEndEvent" should "serialize to JSON" in {
    val uuid = UUID.randomUUID.toString
    val metricRecord = PipelineEndEvent(
      pipeline_id = "pipeline1",
      timestamp = 42,
      event_id = uuid
    )
    val jsonString = metricRecord.toJson.toString
    val jsonMap = mapFromJson(jsonString)
    val expectedMap = Map(
      "pipeline_id" -> "pipeline1",
      "timestamp" -> 42,
      "event_type" -> "PipelineEnd",
      "event_id" -> uuid
    )
    assert(jsonMap == expectedMap)
  }

  "PipelineEndEvent" should "serialize with no phases" in {
    val uuid = UUID.randomUUID.toString
    val metricRecord = PipelineEndEvent(
      pipeline_id = "pipeline1",
      timestamp = 42,
      event_id = uuid
    )
    val jsonString = metricRecord.toJson.toString
    val jsonMap = mapFromJson(jsonString)
    val expectedMap = Map(
      "pipeline_id" -> "pipeline1",
      "timestamp" -> 42,
      "event_type" -> "PipelineEnd",
      "event_id" -> uuid
    )
    assert(jsonMap == expectedMap)
  }

  "PipelineEndEvent" should "deserialize from JSON" in {
    val uuid = UUID.randomUUID.toString
    val metricRecord = PipelineEndEvent(
      pipeline_id = "pipeline1",
      timestamp = 42,
      event_id = uuid
    )
    val jsonString = s"""{
        "pipeline_id": "pipeline1",
        "timestamp": 42,
        "event_type": "PipelineEnd",
        "event_id": "${uuid}"
    }"""
    val parsedMetricRecord = jsonString.parseJson.convertTo[PipelineEndEvent]
    assert(parsedMetricRecord == metricRecord)
  }

  "BatchStartEvent" should "serialize to JSON" in {
    val uuid = UUID.randomUUID.toString
    val metricRecord = BatchStartEvent(
      batch_id = "batch1",
      batch_type = PipelineBatchType.Normal,
      pipeline_id = "pipeline1",
      timestamp = 42,
      event_id = uuid
    )
    val jsonString = metricRecord.toJson.toString
    val jsonMap = mapFromJson(jsonString)
    val expectedMap = Map(
      "batch_id" -> "batch1",
      "batch_type" -> "Normal",
      "pipeline_id" -> "pipeline1",
      "timestamp" -> 42,
      "event_type" -> "BatchStart",
      "event_id" -> uuid
      )
    assert(jsonMap == expectedMap)
  }

  "BatchStartEvent" should "serialize with no phases" in {
    val uuid = UUID.randomUUID.toString
    val metricRecord = BatchStartEvent(
      batch_id = "batch1",
      batch_type = PipelineBatchType.Normal,
      pipeline_id = "pipeline1",
      timestamp = 42,
      event_id = uuid
    )
    val jsonString = metricRecord.toJson.toString
    val jsonMap = mapFromJson(jsonString)
    val expectedMap = Map(
      "batch_id" -> "batch1",
      "batch_type" -> "Normal",
      "pipeline_id" -> "pipeline1",
      "timestamp" -> 42,
      "event_type" -> "BatchStart",
      "event_id" -> uuid
    )
    assert(jsonMap == expectedMap)
  }

  "BatchStartEvent" should "deserialize from JSON" in {
    val uuid = UUID.randomUUID.toString
    val metricRecord = BatchStartEvent(
      batch_id = "batch1",
      batch_type = PipelineBatchType.Normal,
      pipeline_id = "pipeline1",
      timestamp = 42,
      event_id = uuid
    )

    val jsonString = s"""{
        "batch_id": "batch1",
        "batch_type": "Normal",
        "pipeline_id": "pipeline1",
        "timestamp": 42,
        "event_type": "BatchStart",
        "event_id": "${uuid}"
    }"""
    val parsedMetricRecord = jsonString.parseJson.convertTo[BatchStartEvent]
    assert(parsedMetricRecord == metricRecord)
  }

  "BatchStartEvent" should "be preserved through a round trip" in {
    val uuid = UUID.randomUUID.toString
    val metricRecord1 = BatchStartEvent(
      batch_id = "batch1",
      batch_type = PipelineBatchType.Normal,
      pipeline_id = "pipeline1",
      timestamp = 42,
      event_id = uuid
    )
    val metricRecord2 = metricRecord1.toJson.toString.parseJson.convertTo[BatchStartEvent]
    assert(metricRecord1 == metricRecord2)
  }

  "BatchEndEvent" should "serialize to JSON" in {
    val uuid = UUID.randomUUID.toString
    val metricRecord = BatchEndEvent(
      event_id = uuid,
      batch_id = "batch1",
      batch_type = PipelineBatchType.Normal,
      pipeline_id = "pipeline1",
      timestamp = 42,
      success = true,
      task_errors = Some(List(TaskErrorRecord(0, 0, 0, 0, Some("error")))),
      extract = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("extract error"),
        columns = Some(List(("column1", "string"))),
        records = Some(List(List("extract record 1"))),
        logs = Some(List("extract log 1"))
      )),
      transform = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("transform error"),
        columns = Some(List(("column1", "string"), ("column2", "string"))),
        records = Some(List(List("transform record 1", "transform record 2"))),
        logs = Some(List("transform log 1"))
      )),
      load = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("load error"),
        columns = Some(List(("column1", "string"), ("column2", "string"))),
        records = Some(List(List("load record 1", "load record 2"))),
        logs = Some(List("load log 1"))
      ))
    )
    val jsonString = metricRecord.toJson.toString
    val jsonMap = mapFromJson(jsonString)
    val expectedMap = Map(
      "batch_id" -> "batch1",
      "batch_type" -> "Normal",
      "pipeline_id" -> "pipeline1",
      "timestamp" -> 42,
      "success" -> true,
      "task_errors" -> List(
        Map(
          "job_id" -> 0,
          "stage_id" -> 0,
          "task_id" -> 0,
          "finish_time" -> 0,
          "error" -> "error"
        )
      ),
      "extract" -> Map(
        "start" -> 1,
        "stop" -> 2,
        "count" -> 100,
        "error" -> "extract error",
        "columns" -> List(List("column1", "string")),
        "records" -> List(List("extract record 1")),
        "logs" -> List("extract log 1")
      ),
      "transform" -> Map(
        "start" -> 1,
        "stop" -> 2,
        "count" -> 100,
        "error" -> "transform error",
        "columns" -> List(List("column1", "string"), List("column2", "string")),
        "records" -> List(List("transform record 1", "transform record 2")),
        "logs" -> List("transform log 1")
      ),
      "load" -> Map(
        "start" -> 1,
        "stop" -> 2,
        "count" -> 100,
        "error" -> "load error",
        "columns" -> List(List("column1", "string"), List("column2", "string")),
        "records" -> List(List("load record 1", "load record 2")),
        "logs" -> List("load log 1")
      ),
      "event_type" -> "BatchEnd",
      "event_id" -> uuid
    )
    assert(jsonMap == expectedMap)
  }

  "BatchEndEvent" should "serialize with no phases" in {
    val uuid = UUID.randomUUID.toString
    val metricRecord = BatchEndEvent(
      batch_id = "batch1",
      batch_type = PipelineBatchType.Normal,
      pipeline_id = "pipeline1",
      timestamp = 42,
      success = true,
      task_errors = None,
      extract = None,
      transform = None,
      load = None,
      event_id = uuid
    )
    val jsonString = metricRecord.toJson.toString
    val jsonMap = mapFromJson(jsonString)
    val expectedMap = Map(
      "batch_id" -> "batch1",
      "batch_type" -> "Normal",
      "pipeline_id" -> "pipeline1",
      "timestamp" -> 42,
      "success" -> true,
      "event_type" -> "BatchEnd",
      "event_id" -> uuid
    )
    assert(jsonMap == expectedMap)
  }

  it should "deserialize from JSON" in {
    val uuid = UUID.randomUUID.toString
    val metricRecord = BatchEndEvent(
      event_id = uuid,
      batch_id = "batch1",
      batch_type = PipelineBatchType.Normal,
      pipeline_id = "pipeline1",
      timestamp = 42,
      success = true,
      task_errors = None,
      extract = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("extract error"),
        columns = Some(List(("column1", "string"))),
        records = Some(List(List("extract record 1"))),
        logs = Some(List("extract log 1"))
      )),
      transform = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("transform error"),
        columns = Some(List(("column1", "string"), ("column2", "string"))),
        records = Some(List(List("transform record 1", "transform record 2"))),
        logs = Some(List("transform log 1"))
      )),
      load = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("load error"),
        columns = Some(List(("column1", "string"), ("column2", "string"))),
        records = Some(List(List("load record 1", "load record 2"))),
        logs = Some(List("load log 1"))
      ))
    )

    val jsonString = s"""{
        "batch_id": "batch1",
        "batch_type": "Normal",
        "pipeline_id": "pipeline1",
        "timestamp": 42,
        "success": true,
        "extract": {
            "start": 1,
            "stop": 2,
            "count": 100,
            "error": "extract error",
            "columns": [ [ "column1", "string" ] ],
            "records": [
                [ "extract record 1" ]
            ],
            "logs": [
                "extract log 1"
            ]
        },
        "transform": {
            "start": 1,
            "stop": 2,
            "count": 100,
            "error": "transform error",
            "columns": [ [ "column1", "string" ], [ "column2", "string" ] ],
            "records": [
                [ "transform record 1", "transform record 2" ]
            ],
            "logs": [
                "transform log 1"
            ]
        },
        "load": {
            "start": 1,
            "stop": 2,
            "count": 100,
            "error": "load error",
            "columns": [ [ "column1", "string" ], [ "column2", "string" ] ],
            "records": [
                [ "load record 1", "load record 2" ]
            ],
            "logs": [
                "load log 1"
            ]
        },
        "event_type": "BatchEnd",
        "event_id": "${uuid}"
    }"""
    val parsedMetricRecord = jsonString.parseJson.convertTo[BatchEndEvent]
    assert(parsedMetricRecord == metricRecord)
  }

  it should "be preserved through a round trip" in {
    val uuid = UUID.randomUUID.toString
    val metricRecord1 = BatchEndEvent(
      event_id = uuid,
      batch_id = "batch1",
      batch_type = PipelineBatchType.Normal,
      pipeline_id = "pipeline1",
      timestamp = 42,
      success = true,
      task_errors = Some(List(TaskErrorRecord(0,1,2,3,Some("error")))),
      extract = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("extract error"),
        columns = Some(List(("column1", "string"))),
        records = Some(List(List("extract record 1"))),
        logs = Some(List("extract log 1"))
      )),
      transform = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("transform error"),
        columns = Some(List(("column1", "string"), ("column2", "string"))),
        records = Some(List(List("transform record 1", "transform record 2"))),
        logs = Some(List("transform log 1"))
      )),
      load = Some(PhaseMetricRecord(
        start = 1,
        stop = 2,
        count = Some(100),
        error = Some("load error"),
        columns = Some(List(("column1", "string"), ("column2", "string"))),
        records = Some(List(List("load record 1", "load record 2"))),
        logs = Some(List("load log 1"))
      ))
    )
    val metricRecord2 = metricRecord1.toJson.toString.parseJson.convertTo[BatchEndEvent]
    assert(metricRecord1 == metricRecord2)
  }
}
