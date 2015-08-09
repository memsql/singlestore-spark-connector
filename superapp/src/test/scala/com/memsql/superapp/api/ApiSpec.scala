package com.memsql.superapp.api

import akka.actor.Props
import com.memsql.spark.etl.api.configs.LoadPhaseKind.LoadPhaseKind
import com.memsql.spark.etl.api.configs.TransformPhaseKind.TransformPhaseKind
import com.memsql.spark.etl.api.configs._
import com.memsql.superapp.{Config, TestKitSpec}
import com.memsql.superapp.api.ApiActor._
import scala.concurrent.duration._
import spray.json._

import scala.util.{Success, Failure}

import ExtractPhaseKind._

//XXX remove all the invalid asserts
class ApiSpec extends TestKitSpec("ApiActorSpec") {
  val apiRef = system.actorOf(Props(classOf[ApiActor], Config()))

  "Api actor" should {
    "respond to ping" in {
      apiRef ! Ping
      expectMsg("pong")
    }

    "have no pipelines to start" in {
      apiRef ! PipelineQuery
      expectMsg(List())

      apiRef ! PipelineGet("pipelinenotthere")
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      apiRef ! PipelineUpdate("pipelinenotthere", PipelineState.STOPPED)
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }
    }

    "accept new pipelines" in {
      val config = PipelineConfig(
        Phase[ExtractPhaseKind](
          ExtractPhaseKind.Kafka,
          ExtractPhase.writeConfig(
            ExtractPhaseKind.Kafka, KafkaExtractConfig("test1", 9092, "topic", None))),
        Phase[TransformPhaseKind](
          TransformPhaseKind.User,
          TransformPhase.writeConfig(
            TransformPhaseKind.User, UserTransformConfig("com.user.TransformClass", "test1"))),
        Phase[LoadPhaseKind](
          LoadPhaseKind.MemSQL,
          LoadPhase.writeConfig(
            LoadPhaseKind.MemSQL, MemSQLLoadConfig("db", "table"))))

      apiRef ! PipelinePut("pipeline1", jar="site.com/foo.jar", batchInterval=10, config=config)
      expectMsg(Success(true))

      apiRef ! PipelinePut("pipeline1", jar="site.com/foo.jar", batchInterval=10, config=config)
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      apiRef ! PipelineQuery
      receiveOne(1.second) match {
        case pipelines:List[_] => assert(pipelines.length == 1)
        case default => fail(s"unexpected response $default")
      }

      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp:Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.pipeline_id == "pipeline1")
          assert(pipeline.state == PipelineState.RUNNING)
          assert(pipeline.jar == "site.com/foo.jar")
          assert(pipeline.batch_interval == 10)
          assert(pipeline.config == config)
          val kafkaConfig = ExtractPhase.readConfig(pipeline.config.extract.kind, pipeline.config.extract.config).asInstanceOf[KafkaExtractConfig]
          assert(kafkaConfig.host == "test1")
          assert(kafkaConfig.port == 9092)
          assert(kafkaConfig.topic == "topic")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      val config2 = config.copy(extract = Phase[ExtractPhaseKind](
          ExtractPhaseKind.User,
          ExtractPhase.writeConfig(
            ExtractPhaseKind.User, UserExtractConfig("com.user.ExtractClass", "test"))))
      apiRef ! PipelinePut("pipeline2", jar="site.com/bar.jar", batchInterval=10, config=config2)
      expectMsg(Success(true))

      val badConfig = config.copy(extract = Phase[ExtractPhaseKind](ExtractPhaseKind.Kafka, """{ "bad_kafka_config": 42 }""".parseJson))
      apiRef ! PipelinePut("pipeline3", jar="site.com/bar.jar", batchInterval=10, config=badConfig)
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      apiRef ! PipelineQuery
      receiveOne(1.second) match {
        case pipelines:List[_] => assert(pipelines.length == 2)
        case default => fail(s"unexpected response $default")
      }

      apiRef ! PipelineGet("pipeline2")
      receiveOne(1.second) match {
        case resp:Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.pipeline_id == "pipeline2")
          assert(pipeline.state == PipelineState.RUNNING)
          assert(pipeline.jar == "site.com/bar.jar")
          assert(pipeline.config == config2)
          val userConfig = ExtractPhase.readConfig(pipeline.config.extract.kind, pipeline.config.extract.config).asInstanceOf[UserExtractConfig]
          assert(userConfig.value == "test")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }
    }

    "allow updates to pipelines" in {
      apiRef ! PipelineUpdate("pipeline1", state=PipelineState.STOPPED)
      expectMsg(Success(true))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.state == PipelineState.STOPPED)
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      //no-op updates return false
      apiRef ! PipelineUpdate("pipeline1", state=PipelineState.STOPPED)
      expectMsg(Success(false))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.state == PipelineState.STOPPED)
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      apiRef ! PipelineUpdate("pipeline1", state=PipelineState.ERROR, error=Some("something crashed"))
      expectMsg(Success(true))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.state == PipelineState.ERROR)
          assert(pipeline.error == Some("something crashed"))
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }


      // updates to batch interval should only be accepted if interval is positive and non-zero
      apiRef ! PipelineUpdate("pipeline1", batchInterval = 1234)
      expectMsg(Success(true))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.batch_interval == 1234)
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      apiRef ! PipelineUpdate("pipeline1", batchInterval = -1234)
      receiveOne(1.second) match {
        case resp: Success[_] => assert(!resp.get.asInstanceOf[Boolean])
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.batch_interval == 1234)
        case Failure(err) => fail(s"unexpected response $err")
      }

      //an update request from the api must be validate and cannot perform all updates
      apiRef ! PipelineUpdate("pipeline1", state=PipelineState.RUNNING, _validate=true)
      expectMsg(Success(false))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.state == PipelineState.ERROR)
          assert(pipeline.error == Some("something crashed"))
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      // Updating configs should be allowed
      val newConfig = PipelineConfig(
        Phase[ExtractPhaseKind](
          ExtractPhaseKind.Kafka,
          ExtractPhase.writeConfig(
            ExtractPhaseKind.Kafka, KafkaExtractConfig("test1", 9092, "test2", None))),
        Phase[TransformPhaseKind](
          TransformPhaseKind.User,
          TransformPhase.writeConfig(
            TransformPhaseKind.User, UserTransformConfig("com.user.TransformClass", "test1"))),
        Phase[LoadPhaseKind](
          LoadPhaseKind.MemSQL,
          LoadPhase.writeConfig(
            LoadPhaseKind.MemSQL, MemSQLLoadConfig("db", "table"))))

      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.config != newConfig)
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      apiRef ! PipelineUpdate("pipeline1", config=newConfig)
      expectMsg(Success(true))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.config == newConfig)
          assert(pipeline.config.extract.kind == ExtractPhaseKind.Kafka)
          val kafkaConfig = ExtractPhase.readConfig(pipeline.config.extract.kind, pipeline.config.extract.config).asInstanceOf[KafkaExtractConfig]
          assert(kafkaConfig.host == "test1")
          assert(kafkaConfig.port == 9092)
          assert(kafkaConfig.topic == "test2")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      //no-op updates to configs should return false
      apiRef ! PipelineUpdate("pipeline1", config=newConfig)
      expectMsg(Success(false))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.config == newConfig)
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      // Configs that do not deserialize should be rejected.
      val badConfig = newConfig.copy(extract = Phase[ExtractPhaseKind](
          ExtractPhaseKind.Kafka, """{ "bad_kafka_config": 42 }""".parseJson))
      apiRef ! PipelineUpdate("pipeline1", config=badConfig)
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }
    }

    "allow deleting pipelines" in {
      apiRef ! PipelineDelete("pipeline2")
      expectMsg(Success(true))

      apiRef ! PipelineQuery
      receiveOne(1.second) match {
        case pipelines:List[_] => assert(pipelines.length == 1)
        case default => fail(s"unexpected response $default")
      }

      apiRef ! PipelineGet("pipeline2")
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      apiRef ! PipelineDelete("pipeline2")
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }
    }
  }
}
