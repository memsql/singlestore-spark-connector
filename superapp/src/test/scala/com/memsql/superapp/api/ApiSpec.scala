package com.memsql.superapp.api

import akka.actor.Props
import com.memsql.spark.etl.api._
import com.memsql.superapp.{Config, TestKitSpec}
import com.memsql.superapp.api.ApiActor._
import scala.concurrent.duration._

import scala.util.{Success, Failure}

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
      apiRef ! PipelinePut("pipeline1", jar="site.com/foo.jar", main_class="com.foo.FooMain")
      expectMsg(Success(true))

      apiRef ! PipelinePut("pipeline1", jar="site.com/foo.jar", main_class="com.foo.FooMain")
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
          assert(pipeline.main_class == "com.foo.FooMain")
          assert(pipeline.config.extract_config.get.`type` == PipelineExtractType.USER)
          assert(pipeline.config.extract_config.get.config.asInstanceOf[PipelineUserExtractConfigData].value == "")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      val config = PipelineConfig(
        Some(PipelineExtractConfig(
          PipelineExtractType.KAFKA,
          PipelineKafkaExtractConfigData(
            "test1", "test2", Map()))),
        None,
        None)
      apiRef ! PipelinePut("pipeline2", jar="site.com/bar.jar", main_class="com.bar.BarMain", config=config)
      expectMsg(Success(true))

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
          assert(pipeline.main_class == "com.bar.BarMain")
          assert(pipeline.config.extract_config.get.`type` == PipelineExtractType.KAFKA)
          assert(pipeline.config.extract_config.get.config.asInstanceOf[PipelineKafkaExtractConfigData].zk_quorum == "test1")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }
    }

    "allow updates to pipelines" in {
      apiRef ! PipelineUpdate("pipeline1", PipelineState.STOPPED)
      expectMsg(Success(true))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.state == PipelineState.STOPPED)
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      //no-op updates return false
      apiRef ! PipelineUpdate("pipeline1", PipelineState.STOPPED)
      expectMsg(Success(false))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.state == PipelineState.STOPPED)
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      apiRef ! PipelineUpdate("pipeline1", PipelineState.ERROR, error="something crashed")
      expectMsg(Success(true))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.state == PipelineState.ERROR)
          assert(pipeline.error == "something crashed")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      //an update request from the api must be validate and cannot perform all updates
      apiRef ! PipelineUpdate("pipeline1", PipelineState.RUNNING, _validate=true)
      expectMsg(Success(false))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.state == PipelineState.ERROR)
          assert(pipeline.error == "something crashed")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      // Updating configs should be allowed
      val config = PipelineConfig(
        Some(PipelineExtractConfig(
          PipelineExtractType.KAFKA,
          PipelineKafkaExtractConfigData(
            "test1", "test2", Map()))),
        None,
        None)
      apiRef ! PipelineUpdate("pipeline1", config=config)
      expectMsg(Success(true))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.config.extract_config.get.`type` == PipelineExtractType.KAFKA)
          assert(pipeline.config.extract_config.get.config.asInstanceOf[PipelineKafkaExtractConfigData].zk_quorum == "test1")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      //no-op updates to configs should return false
      apiRef ! PipelineUpdate("pipeline1", config=config)
      expectMsg(Success(false))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.config.extract_config.get.`type` == PipelineExtractType.KAFKA)
          assert(pipeline.config.extract_config.get.config.asInstanceOf[PipelineKafkaExtractConfigData].zk_quorum == "test1")
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
