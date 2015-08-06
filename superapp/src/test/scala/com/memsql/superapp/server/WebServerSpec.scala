package com.memsql.superapp.server

import akka.actor.Props
import com.memsql.spark.etl.api.configs._
import ExtractPhaseKind._
import TransformPhaseKind._
import LoadPhaseKind._
import com.memsql.superapp.{Config, UnitSpec}
import spray.http.HttpEntity
import spray.http.ContentTypes._
import spray.json._
import spray.testkit.ScalatestRouteTest
import spray.http.StatusCodes._
import com.memsql.superapp.api._
import com.memsql.spark.etl.api.configs.PipelineConfigVersion.CurrentPipelineConfigVersion
import ApiJsonProtocol._

class WebServerSpec extends UnitSpec with ScalatestRouteTest with WebService {

  def actorRefFactory = system
  val apiRef = system.actorOf(Props(classOf[ApiActor], Config()), "api")

  val emptyConfig = PipelineConfig(Some(Phase[ExtractPhaseKind](ExtractPhaseKind.User,
                                    ExtractPhase.writeConfig(
                                      ExtractPhaseKind.User, UserExtractConfig("")))),
                                    Some(Phase[TransformPhaseKind](
                                      TransformPhaseKind.User,
                                      TransformPhase.writeConfig(
                                        TransformPhaseKind.User, UserTransformConfig("")))),
                                    Some(Phase[LoadPhaseKind](
                                      LoadPhaseKind.User,
                                      LoadPhase.writeConfig(
                                        LoadPhaseKind.User, UserLoadConfig("")))),
                                    config_version=CurrentPipelineConfigVersion)
  val emptyConfigEntity = HttpEntity(`application/json`, emptyConfig.toJson.toString)
  val basicConfig = PipelineConfig(Some(Phase[ExtractPhaseKind](ExtractPhaseKind.User,
                                    ExtractPhase.writeConfig(
                                    ExtractPhaseKind.User, UserExtractConfig("extract")))),
                                   Some(Phase[TransformPhaseKind](
                                    TransformPhaseKind.User,
                                    TransformPhase.writeConfig(
                                    TransformPhaseKind.User, UserTransformConfig("transform")))),
                                  Some(Phase[LoadPhaseKind](
                                    LoadPhaseKind.User,
                                    LoadPhase.writeConfig(
                                    LoadPhaseKind.User, UserLoadConfig("load")))),
                                  config_version=CurrentPipelineConfigVersion)
  val basicConfigEntity = HttpEntity(`application/json`, basicConfig.toJson.toString)
  val basicPipeline = Pipeline("asdf", PipelineState.RUNNING, "asdf.jar", "com.asdf.Asdf", basicConfig)
  def putPipeline(useEmptyConfig: Boolean = false): Unit = {
    val entity = if (useEmptyConfig) emptyConfigEntity else basicConfigEntity
    Post("/pipeline/put?pipeline_id=asdf&jar=asdf.jar&main_class=com.asdf.Asdf", entity) ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(true)).toString)
      assert(status == OK)
    }
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
    Post("/pipeline/put?pipeline_id=asdf&jar=asdf.jar&main_class=com.asdf.Asdf", basicConfigEntity) ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(true)).toString)
      assert(status == OK)
    }
  }

  it should "reject POST if the pipeline id already exists" in {
    putPipeline()

    Post("/pipeline/put?pipeline_id=asdf&jar=asdf2.jar&main_class=com.asdf.Asdf2", basicConfigEntity) ~> route ~> check {
      assert(responseAs[String] == "pipeline with id asdf already exists")
      assert(status == BadRequest)
    }
  }

  it should "reject POST if parameters are missing" in {
    Post("/pipeline/put") ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("missing required query parameter 'pipeline_id'"))
      assert(status == NotFound)
    }

    Post("/pipeline/put?jar=asdf&main_class=asdf2", basicConfigEntity) ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("missing required query parameter 'pipeline_id'"))
      assert(status == NotFound)
    }

    Post("/pipeline/put?pipeline_id=asdf&main_class=asdf2", basicConfigEntity) ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("missing required query parameter 'jar'"))
      assert(status == NotFound)
    }

    Post("/pipeline/put?pipeline_id=asdf&jar=asdf2.jar", basicConfigEntity) ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("missing required query parameter 'main_class'"))
      assert(status == NotFound)
    }

    Post("/pipeline/put?pipeline_id=asdf&jar=asdf2.jar&main_class=com.asdf.Asdf2") ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("entity expected"))
      assert(status == BadRequest)
    }
  }

  it should "reject other methods" in {
    Get("/pipeline/put") ~> sealRoute(route) ~> check { assert(status == NotFound) }
    Put("/pipeline/put") ~> sealRoute(route) ~> check { assert(status == NotFound) }
    Patch("/pipeline/put") ~> sealRoute(route) ~> check { assert(status == NotFound) }
    Delete("/pipeline/put") ~> sealRoute(route) ~> check { assert(status == NotFound) }

    Get("/pipeline/put?pipeline_id=asdf&jar=asdf.jar&main_class=com.asdf.Asdf", basicConfigEntity) ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Put("/pipeline/put?pipeline_id=asdf&jar=asdf.jar&main_class=com.asdf.Asdf", basicConfigEntity) ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Patch("/pipeline/put?pipeline_id=asdf&jar=asdf.jar&main_class=com.asdf.Asdf", basicConfigEntity) ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
    Delete("/pipeline/put?pipeline_id=asdf&jar=asdf.jar&main_class=com.asdf.Asdf", basicConfigEntity) ~> sealRoute(route) ~> check {
      assert(status == MethodNotAllowed)
    }
  }

  "WebServer /pipeline/get" should "respond to a valid GET" in {
    putPipeline()

    Get("/pipeline/get?pipeline_id=asdf") ~> route ~> check {
      assert(responseAs[String] == basicPipeline.toJson.toString)
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

  "WebServer /pipeline/delete" should "respond to a valid DELETE" in {
    putPipeline()

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
    putPipeline(useEmptyConfig = true)

    // changing pipeline's active state should return true
    Patch("/pipeline/update?pipeline_id=asdf&active=false") ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(true)).toString)
      assert(status == OK)
    }

    // pipeline should now be stopped and should still have an empty pipeline config
    Get("/pipeline/get?pipeline_id=asdf") ~> route ~> check {
      val pipeline = responseAs[String].parseJson.convertTo[Pipeline]
      assert(pipeline.state == PipelineState.STOPPED)
      assert(pipeline.config == emptyConfig)
      assert(status == OK)
    }

    // updating config and active state should return true
    Patch("/pipeline/update?pipeline_id=asdf&active=true", basicConfigEntity) ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(true)).toString)
      assert(status == OK)
    }

    // pipeline should now be running and have the new config
    Get("/pipeline/get?pipeline_id=asdf") ~> route ~> check {
      val pipeline = responseAs[String].parseJson.convertTo[Pipeline]
      assert(pipeline.state == PipelineState.RUNNING)
      assert(pipeline.config == basicConfig)
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
      assert(pipeline.config == basicConfig)
      assert(status == OK)
    }

    // no-op updates with an identical config entity should also return false
    Patch("/pipeline/update?pipeline_id=asdf&active=true", basicConfigEntity) ~> route ~> check {
      assert(responseAs[String] == JsObject("success" -> JsBoolean(false)).toString)
      assert(status == OK)
    }

    // pipeline should still be in the same state
    Get("/pipeline/get?pipeline_id=asdf") ~> route ~> check {
      val pipeline = responseAs[String].parseJson.convertTo[Pipeline]
      assert(pipeline.state == PipelineState.RUNNING)
      assert(pipeline.config == basicConfig)
      assert(status == OK)
    }

    // updating only the config should return true
    val newConfig = basicConfig.copy(extract = Some(Phase[ExtractPhaseKind](
                                                ExtractPhaseKind.Kafka,
                                                ExtractPhase.writeConfig(
                                                  ExtractPhaseKind.Kafka,
                                                  KafkaExtractConfig(
                                                    "broker",
                                                    List("topic1", "topic2"),
                                                    Some(KafkaExtractOutputType.String))))))
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

    Patch("/pipeline/update?pipeline_id=asdf") ~> sealRoute(route) ~> check {
      assert(responseAs[String].contains("missing required query parameter 'active'"))
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

    putPipeline()

    Get("/pipeline/query") ~> route ~> check {
      assert(responseAs[String] == JsArray(basicPipeline.toJson).toString)
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
