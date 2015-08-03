package com.memsql.superapp.api

import akka.actor.Actor
import akka.util.Timeout
import scala.concurrent.duration._
import com.memsql.superapp.Config
import com.memsql.superapp.util.BaseException
import spray.json._

import scala.collection.mutable
import scala.util.{Failure, Success}

object PipelineState extends Enumeration {
  type PipelineState = Value
  val RUNNING, STOPPED, ERROR = Value
}

import PipelineState._

case class Pipeline(pipeline_id: String,
                    var state: PipelineState,
                    jar: String,
                    main_class: String,
                    var error: String = null)

object ApiJsonProtocol extends DefaultJsonProtocol {
  implicit object pipelineFormat extends RootJsonFormat[Pipeline] {
    override def read(value: JsValue): Pipeline = {
      value.asJsObject.getFields("pipeline_id", "state", "jar", "main_class", "error") match {
        case Seq(JsString(pipeline_id), JsString(state), JsString(jar), JsString(main_class), JsString(error)) => {
          PipelineState.values.find(_.toString == state) match {
            case s: Some[PipelineState] => Pipeline(pipeline_id, s.get, jar, main_class, error)
            case None => deserializationError(s"pipeline state $state does not exist")
          }
        }
        case _ => deserializationError("pipeline expected")
      }
    }

    override def write(obj: Pipeline): JsValue = {
      JsObject(
        "pipeline_id" -> JsString(obj.pipeline_id),
        "state" -> JsString(obj.state.toString),
        "active" -> JsBoolean(obj.state == PipelineState.RUNNING),
        "jar" -> JsString(obj.jar),
        "main_class" -> JsString(obj.main_class),
        "error" -> (if (obj.error == null) JsNull else JsString(obj.error))
      )
    }
  }

}

case class ApiException(message: String) extends BaseException(message: String)

object ApiActor {
  case object Ping
  case object PipelineQuery
  case class PipelineGet(pipeline_id: String)
  case class PipelinePut(pipeline_id: String, jar: String, main_class: String)
  case class PipelineUpdate(pipeline_id: String, state: PipelineState, error: String = null, _validate: Boolean = false)
  case class PipelineDelete(pipeline_id: String)
  implicit val timeout = Timeout(5.seconds)
}

class ApiActor(config: Config) extends Actor {
  import ApiActor._

  private var pipelines = new mutable.ListBuffer[Pipeline]()

  override def receive: Receive = {
    case Ping => sender ! "pong"
    case PipelineQuery => sender ! pipelines.toList
    case PipelineGet(pipeline_id) => {
      pipelines.find(_.pipeline_id == pipeline_id) match {
        case Some(pipeline) => sender ! Success(pipeline)
        case None => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case PipelinePut(pipeline_id, jar, main_class) => {
      pipelines.find(_.pipeline_id == pipeline_id) match {
        case p: Some[Pipeline] => sender ! Failure(ApiException(s"pipeline with id $pipeline_id already exists"))
        case _ => {
          pipelines += Pipeline(pipeline_id, RUNNING, jar, main_class)
          sender ! Success(true)
        }
      }
    }
    case PipelineUpdate(pipeline_id, state, error, _validate) => {
      pipelines.find(_.pipeline_id == pipeline_id) match {
        case p: Some[Pipeline] => {
          val pipeline = p.get
          val oldState = pipeline.state
          pipeline.state = (pipeline.state, state, _validate) match {
            case (_, _, false) => state
            case (RUNNING, STOPPED, _) => state
            case (STOPPED, RUNNING, _) => state
            case default => pipeline.state
          }

          if (oldState != pipeline.state) {
            pipeline.error = error
            sender ! Success(true)
          } else {
            sender ! Success(false)
          }
        }
        case _ => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case PipelineDelete(pipeline_id) => {
      pipelines.find(_.pipeline_id == pipeline_id) match {
        case p: Some[Pipeline] => {
          pipelines -= p.get
          sender ! Success(true)
        }
        case _ => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case default => sender ! ApiException("invalid api message type")
  }
}
