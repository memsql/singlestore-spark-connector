package com.memsql.spark.interface.server

import akka.actor.Props
import com.memsql.spark.etl.api.UserExtractConfig
import com.memsql.spark.etl.api.configs._
import ExtractPhaseKind._
import TransformPhaseKind._
import LoadPhaseKind._
import com.memsql.spark.interface.api._
import com.memsql.spark.interface._
import com.memsql.spark.phases.{JsonTransformConfig, KafkaExtractConfig}
import com.memsql.spark.phases.configs.{ExtractPhase}
import spray.http.HttpEntity
import spray.http.ContentTypes._
import spray.json._
import spray.testkit.ScalatestRouteTest
import spray.http.StatusCodes._
import com.memsql.spark.etl.api.configs.PipelineConfigVersion.CurrentPipelineConfigVersion
import ApiJsonProtocol._

class WebServerSpec extends UnitSpec with ScalatestRouteTest with WebService {
  override val appWebUIPort = -1

  def actorRefFactory = system
  val apiRef = system.actorOf(Props[ApiActor], "api")

  val kafkaConfig = PipelineConfig(Phase[ExtractPhaseKind](
                                    ExtractPhaseKind.Kafka,
                                    ExtractPhase.writeConfig(
                                      ExtractPhaseKind.Kafka, KafkaExtractConfig("test1", 9092, "topic"))),
                                   Phase[TransformPhaseKind](
                                    TransformPhaseKind.Json,
                                    TransformPhase.writeConfig(
                                      TransformPhaseKind.Json, JsonTransformConfig("data"))),
                                  Phase[LoadPhaseKind](
                                    LoadPhaseKind.MemSQL,
                                    LoadPhase.writeConfig(
                                      LoadPhaseKind.MemSQL, MemSQLLoadConfig("db", "table", None, None))),
                                  config_version=CurrentPipelineConfigVersion)

  val userConfig = kafkaConfig.copy(extract = Phase[ExtractPhaseKind](
                                                ExtractPhaseKind.User,
                                                ExtractPhase.writeConfig(
                                                  ExtractPhaseKind.User, UserExtractConfig("com.user.Extract", JsString("")))))
  val kafkaConfigEntity = HttpEntity(`application/json`, kafkaConfig.toJson.toString)
  val basicPipeline = Pipeline("asdf", state=PipelineState.RUNNING, batch_interval=100, config=kafkaConfig, last_updated=0)
  def putPipeline(pipeline: Pipeline): Unit = {
    val configEntity = HttpEntity(`application/json`, pipeline.config.toJson.toString)
    Post(s"/pipeline/put?pipeline_id=${pipeline.pipeline_id}&batch_interval=${pipeline.batch_interval}", configEntity) ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(true)).toString)
      assert(status == OK)
    }
  }

  "WebServer /version" should "respond to GET with info" in {
    Get("/version") ~> route ~> check {
      val resp = responseAs[String].parseJson.asJsObject
      assert(resp.getFields("name")(0) == JsString("MemSQL Spark Interface"))
      assert(resp.getFields("version")(0) == JsString(Main.VERSION))
      assert(status == OK)
    }
  }

  it should "reject other methods" in {
    Post("/version") ~> sealRoute(route) ~> check { assert(status == MethodNotAllowed) }
    Put("/version") ~> sealRoute(route) ~> check { assert(status == MethodNotAllowed) }
    Patch("/version") ~> sealRoute(route) ~> check { assert(status == MethodNotAllowed) }
    Delete("/version") ~> sealRoute(route) ~> check { assert(status == MethodNotAllowed) }
  }

  "WebServer /ping" should "respond to GET with 'pong'" in {
    Get("/ping") ~> route ~> check {
      assert(responseAs[String] == "\"pong\"")
      assert(status == OK)
    }
  }

  it should "reject other methods" in {
    Post("/ping") ~> sealRoute(route) ~> check { assert(status == MethodNotAllowed) }
    Put("/ping") ~> sealRoute(route) ~> check { assert(status == MethodNotAllowed) }
    Patch("/ping") ~> sealRoute(route) ~> check { assert(status == MethodNotAllowed) }
    Delete("/ping") ~> sealRoute(route) ~> check { assert(status == MethodNotAllowed) }
  }

  "WebServer /pipeline/put" should "respond to a valid POST" in {
    Post("/pipeline/put?pipeline_id=asdf&batch_interval=10", kafkaConfigEntity) ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(true)).toString)
      assert(status == OK)
    }
  }

  it should "reject POST if the pipeline id already exists" in {
    putPipeline(basicPipeline)

    Post("/pipeline/put?pipeline_id=asdf&batch_interval=12", kafkaConfigEntity) ~> route ~> check {
      assert(responseAs[String] == "pipeline with id asdf already exists")
      assert(status == BadRequest)
    }
  }

  it should "reject POST if parameters are missing or invalid" in {
    Post("/pipeline/put") ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("missing required query parameter 'pipeline_id'"))
      assert(status == NotFound)
    }

    Post("/pipeline/put?main_class=asdf2", kafkaConfigEntity) ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("missing required query parameter 'pipeline_id'"))
      assert(status == NotFound)
    }

    Post("/pipeline/put?pipeline_id=asdf", kafkaConfigEntity) ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("missing required query parameter 'batch_interval'"))
      assert(status == NotFound)
    }

    Post("/pipeline/put?pipeline_id=asdf&batch_interval=1234") ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("entity expected"))
      assert(status == BadRequest)
    }

    Post("/pipeline/put?pipeline_id=asdf&batch_interval=0", kafkaConfigEntity) ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("batch_interval must be at least 1 second"))
      assert(status == BadRequest)
    }
  }

  it should "reject other methods" in {
    Get("/pipeline/put") ~> sealRoute(route) ~> check { assert(status == NotFound) }
    Put("/pipeline/put") ~> sealRoute(route) ~> check { assert(status == NotFound) }
    Patch("/pipeline/put") ~> sealRoute(route) ~> check { assert(status == NotFound) }
    Delete("/pipeline/put") ~> sealRoute(route) ~> check { assert(status == NotFound) }

    Get("/pipeline/put?pipeline_id=asdf&batch_interval=1234", kafkaConfigEntity) ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Put("/pipeline/put?pipeline_id=asdf&batch_interval=1234", kafkaConfigEntity) ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Patch("/pipeline/put?pipeline_id=asdf&batch_interval=1234", kafkaConfigEntity) ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Delete("/pipeline/put?pipeline_id=asdf&batch_interval=1234", kafkaConfigEntity) ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
  }

  "WebServer /pipeline/get" should "respond to a valid GET" in {
    putPipeline(basicPipeline)

    Get("/pipeline/get?pipeline_id=asdf") ~> route ~> check {
      //we ignore the value of last_updated here
      val webPipelineJson = responseAs[String].parseJson.asJsObject
      val basicPipelineJson = basicPipeline.toJson.asJsObject
      assert(webPipelineJson.fields.filterKeys(_ != "last_updated") == basicPipelineJson.fields.filterKeys(_ != "last_updated"))

      //and the value of last_updated should have changed
      val last_updated = webPipelineJson.getFields("last_updated")(0).toString.toLong
      assert(last_updated > basicPipeline.last_updated)
      assert(last_updated < System.currentTimeMillis)

      assert(status == OK)
    }
  }

  it should "reject GET if pipeline id doesn't exist" in {
    Get("/pipeline/get?pipeline_id=doesntexist") ~> sealRoute(route) ~> check {
      assert(responseAs[String] == "no pipeline exists with id doesntexist")
      assert(status == NotFound)
    }
  }

  it should "reject GET if parameter is missing" in {
    Get("/pipeline/get") ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("missing required query parameter 'pipeline_id'"))
      assert(status == NotFound)
    }
  }

  it should "reject other methods" in {
    Post("/pipeline/get") ~> sealRoute(route) ~> check { assert(status == NotFound) }
    Put("/pipeline/get") ~> sealRoute(route) ~> check { assert(status == NotFound) }
    Patch("/pipeline/get") ~> sealRoute(route) ~> check { assert(status == NotFound) }
    Delete("/pipeline/get") ~> sealRoute(route) ~> check { assert(status == NotFound) }

    Post("/pipeline/get?pipeline_id=asdf") ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Put("/pipeline/get?pipeline_id=asdf") ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Patch("/pipeline/get?pipeline_id=asdf") ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Delete("/pipeline/get?pipeline_id=asdf") ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
  }

  "WebServer /pipeline/metrics" should "respond to a valid GET" in {
    putPipeline(basicPipeline)

    Get("/pipeline/metrics?pipeline_id=fake") ~> sealRoute(route) ~> check {
      assert(status == NotFound)
    }

    Get("/pipeline/metrics?pipeline_id=asdf") ~> route ~> check {
      val resp = responseAs[String].parseJson.asInstanceOf[JsArray]
      assert(resp.elements.length == 0)
    }

    Get("/pipeline/metrics?pipeline_id=asdf&last_timestamp=105") ~> route ~> check {
      val resp = responseAs[String].parseJson.asInstanceOf[JsArray]
      assert(resp.elements.length == 0)
    }
  }

  it should "reject other methods" in {
    Post("/pipeline/metrics?pipeline_id=asdf") ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Put("/pipeline/metrics?pipeline_id=asdf") ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Patch("/pipeline/metrics?pipeline_id=asdf") ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Delete("/pipeline/metrics?pipeline_id=asdf") ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
  }

  "WebServer /pipeline/delete" should "respond to a valid DELETE" in {
    putPipeline(basicPipeline)

    Delete("/pipeline/delete?pipeline_id=asdf") ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(true)).toString)
      assert(status == OK)
    }

    Get("/pipeline/get?pipeline_id=asdf") ~> sealRoute(route) ~> check {
      assert(responseAs[String] == "no pipeline exists with id asdf")
      assert(status == NotFound)
    }
  }

  it should "reject DELETE if pipeline id doesn't exist" in {
    Delete("/pipeline/delete?pipeline_id=doesntexist") ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("no pipeline exists with id doesntexist"))
      assert(status == NotFound)
    }
  }

  it should "reject DELETE if parameters are missing" in {
    Delete("/pipeline/delete") ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("missing required query parameter 'pipeline_id'"))
      assert(status == NotFound)
    }
  }

  it should "reject other methods" in {
    Get("/pipeline/delete") ~> sealRoute(route) ~> check { assert(status == NotFound) }
    Post("/pipeline/delete") ~> sealRoute(route) ~> check { assert(status == NotFound) }
    Put("/pipeline/delete") ~> sealRoute(route) ~> check { assert(status == NotFound) }
    Patch("/pipeline/delete") ~> sealRoute(route) ~> check { assert(status == NotFound) }

    Get("/pipeline/delete?pipeline_id=asdf") ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Post("/pipeline/delete?pipeline_id=asdf") ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Put("/pipeline/delete?pipeline_id=asdf") ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Patch("/pipeline/delete?pipeline_id=asdf") ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
  }

  "WebServer /pipeline/update" should "respond to a valid PATCH" in {
    putPipeline(basicPipeline.copy(config = userConfig))

    // changing pipeline's active state should return true
    Patch("/pipeline/update?pipeline_id=asdf&active=false") ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(true)).toString)
      assert(status == OK)
    }

    // pipeline should now be stopped and should still have the same pipeline config
    Get("/pipeline/get?pipeline_id=asdf") ~> route ~> check {
      val pipeline = responseAs[String].parseJson.convertTo[Pipeline]
      assert(pipeline.state == PipelineState.STOPPED)
      assert(pipeline.config == userConfig)
      assert(status == OK)
    }

    // updating config and active state should return true
    Patch("/pipeline/update?pipeline_id=asdf&active=true", kafkaConfigEntity) ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(true)).toString)
      assert(status == OK)
    }

    // pipeline should now be running and have the new config
    Get("/pipeline/get?pipeline_id=asdf") ~> route ~> check {
      val pipeline = responseAs[String].parseJson.convertTo[Pipeline]
      assert(pipeline.state == PipelineState.RUNNING)
      assert(pipeline.config == kafkaConfig)
      assert(status == OK)
    }

    // no-op updates to active should return false and if the entity is missing the config shouldn't change
    Patch("/pipeline/update?pipeline_id=asdf&active=true") ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(false)).toString)
      assert(status == OK)
    }

    // pipeline should still be running with the same config
    Get("/pipeline/get?pipeline_id=asdf") ~> route ~> check {
      val pipeline = responseAs[String].parseJson.convertTo[Pipeline]
      assert(pipeline.state == PipelineState.RUNNING)
      assert(pipeline.config == kafkaConfig)
      assert(status == OK)
    }

    // updates to batch_interval should return true
    Patch("/pipeline/update?pipeline_id=asdf&active=true&batch_interval=998") ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(true)).toString)
      assert(status == OK)
    }

    Get("/pipeline/get?pipeline_id=asdf") ~> route ~> check {
      val pipeline = responseAs[String].parseJson.convertTo[Pipeline]
      assert(pipeline.state == PipelineState.RUNNING)
      assert(pipeline.batch_interval == 998)
      assert(status == OK)
    }

    // if active is not specified, thet pipeline's state should not change
    Patch("/pipeline/update?pipeline_id=asdf&batch_interval=999") ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(true)).toString)
      assert(status == OK)
    }

    Get("/pipeline/get?pipeline_id=asdf") ~> route ~> check {
      val pipeline = responseAs[String].parseJson.convertTo[Pipeline]
      assert(pipeline.state == PipelineState.RUNNING)
      assert(pipeline.batch_interval == 999)
      assert(status == OK)
    }

    // no-op updates to config should return false
    Patch("/pipeline/update?pipeline_id=asdf&active=true", kafkaConfigEntity) ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(false)).toString)
      assert(status == OK)
    }

    // pipeline should be running with the same config
    Get("/pipeline/get?pipeline_id=asdf") ~> route ~> check {
      val pipeline = responseAs[String].parseJson.convertTo[Pipeline]
      assert(pipeline.state == PipelineState.RUNNING)
      assert(pipeline.config == kafkaConfig)
      assert(pipeline.batch_interval == 999)
      assert(status == OK)
    }

    val userConfigEntity = HttpEntity(`application/json`, userConfig.toJson.toString)
    Patch("/pipeline/update?pipeline_id=asdf&active=true", userConfigEntity) ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(true)).toString)
      assert(status == OK)
    }

    // pipeline should be running with a new config
    Get("/pipeline/get?pipeline_id=asdf") ~> route ~> check {
      val pipeline = responseAs[String].parseJson.convertTo[Pipeline]
      assert(pipeline.state == PipelineState.RUNNING)
      assert(pipeline.config == userConfig)
      assert(pipeline.batch_interval == 999)
      assert(status == OK)
    }

    // no-op updates to batch_interval should return false
    Patch("/pipeline/update?pipeline_id=asdf&active=true&batch_interval=999") ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(false)).toString)
      assert(status == OK)
    }

    // pipeline should still be running with the same config
    Get("/pipeline/get?pipeline_id=asdf") ~> route ~> check {
      val pipeline = responseAs[String].parseJson.convertTo[Pipeline]
      assert(pipeline.state == PipelineState.RUNNING)
      assert(pipeline.config == userConfig)
      assert(pipeline.batch_interval == 999)
      assert(status == OK)
    }

    // no-op updates to config should return false
    Patch("/pipeline/update?pipeline_id=asdf&active=true", userConfigEntity) ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(false)).toString)
      assert(status == OK)
    }

    // pipeline should still be in the same state
    Get("/pipeline/get?pipeline_id=asdf") ~> route ~> check {
      val pipeline = responseAs[String].parseJson.convertTo[Pipeline]
      assert(pipeline.state == PipelineState.RUNNING)
      assert(pipeline.config == userConfig)
      assert(status == OK)
    }

    // updating only the config should return true
    val newConfig = kafkaConfig.copy(extract = Phase[ExtractPhaseKind](
                                                ExtractPhaseKind.Kafka,
                                                ExtractPhase.writeConfig(
                                                  ExtractPhaseKind.Kafka,
                                                  KafkaExtractConfig("test1", 9092, "topic1"))))
    val newConfigEntity = HttpEntity(`application/json`, newConfig.toJson.toString)

    Patch("/pipeline/update?pipeline_id=asdf&active=true", newConfigEntity) ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(true)).toString)
      assert(status == OK)
    }

    // pipeline should still be running but with the updated config
    Get("/pipeline/get?pipeline_id=asdf") ~> route ~> check {
      val pipeline = responseAs[String].parseJson.convertTo[Pipeline]
      assert(pipeline.state == PipelineState.RUNNING)
      assert(pipeline.config == newConfig)
      assert(status == OK)
    }
  }

  it should "reject PATCH if pipeline id doesn't exist" in {
    Patch("/pipeline/update?pipeline_id=doesntexist&active=false") ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("no pipeline exists with id doesntexist"))
      assert(status == NotFound)
    }
  }

  it should "reject PATCH if parameters are missing" in {
    Patch("/pipeline/update") ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("missing required query parameter 'pipeline_id'"))
      assert(status == NotFound)
    }

    Patch("/pipeline/update?active=true") ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("missing required query parameter 'pipeline_id'"))
      assert(status == NotFound)
    }
  }

  it should "reject other methods" in {
    Get("/pipeline/update") ~> sealRoute(route) ~> check { assert(status == NotFound) }
    Post("/pipeline/update") ~> sealRoute(route) ~> check { assert(status == NotFound) }
    Put("/pipeline/update") ~> sealRoute(route) ~> check { assert(status == NotFound) }
    Delete("/pipeline/update") ~> sealRoute(route) ~> check { assert(status == NotFound) }

    Get("/pipeline/update?pipeline_id=asdf&active=false") ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Post("/pipeline/update?pipeline_id=asdf&active=false") ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Put("/pipeline/update?pipeline_id=asdf&active=false") ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Delete("/pipeline/update?pipeline_id=asdf&active=false") ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
  }

  "WebServer /pipeline/query" should "respond to GET" in {
    Get("/pipeline/query") ~> route ~> check {
      assert(responseAs[String] == JsArray().toString)
      assert(status == OK)
    }

    putPipeline(basicPipeline)

    Get("/pipeline/query") ~> route ~> check {
      //we expect a single pipeline
      val webPipelinesJson = responseAs[String].parseJson.asInstanceOf[JsArray]
      assert(webPipelinesJson.elements.length == 1)

      //whose value should be the same as basicPipeline, excepting last_updated
      val webPipelineJson = webPipelinesJson.elements(0).asJsObject
      val basicPipelineJson = basicPipeline.toJson.asJsObject
      assert(webPipelineJson.fields.filterKeys(_ != "last_updated") == basicPipelineJson.fields.filterKeys(_ != "last_updated"))

      //and the value of last_updated should have changed
      val last_updated = webPipelineJson.getFields("last_updated")(0).toString.toLong
      assert(last_updated > basicPipeline.last_updated)
      assert(last_updated < System.currentTimeMillis)

      assert(status == OK)
    }

    Delete("/pipeline/delete?pipeline_id=asdf") ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(true)).toString)
      assert(status == OK)
    }

    Get("/pipeline/query") ~> route ~> check {
      assert(responseAs[String] == JsArray().toString)
      assert(status == OK)
    }
  }

  it should "reject other methods" in {
    Post("/pipeline/query") ~> sealRoute(route) ~> check { assert(status == MethodNotAllowed) }
    Put("/pipeline/query") ~> sealRoute(route) ~> check { assert(status == MethodNotAllowed) }
    Patch("/pipeline/query") ~> sealRoute(route) ~> check { assert(status == MethodNotAllowed) }
    Delete("/pipeline/query") ~> sealRoute(route) ~> check { assert(status == MethodNotAllowed) }
  }
}
