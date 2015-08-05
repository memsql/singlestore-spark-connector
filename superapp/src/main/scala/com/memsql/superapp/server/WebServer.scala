package com.memsql.superapp.server

import akka.actor.Actor
import akka.pattern.ask
import com.memsql.superapp.api._
import com.memsql.spark.etl.api.configs.PipelineConfig
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport._
import spray.routing.HttpService
import spray.json._
import scala.concurrent.duration._
import akka.util.Timeout
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure, Try}

class WebServer extends Actor with WebService {
  def actorRefFactory = context
  def receive = runRoute(route)
}

trait WebService extends HttpService {
  import com.memsql.superapp.api.ApiJsonProtocol._
  import com.memsql.superapp.api.ApiActor._
  import ooyala.common.akka.web.JsonUtils._

  val api = actorRefFactory.actorSelection("api")
  implicit val timeout = Timeout(5.seconds)

  val route = {
    pathSingleSlash{
      get {
        complete("superapp")
      }
    } ~
    path("ping") {
      get { ctx =>
        val future = (api ? Ping).mapTo[String]
        future.map { resp =>
          ctx.complete(resp.toJson.toString)
        }
      }
    } ~
    path("pipeline" / "query") {
      get { ctx =>
        val future = (api ? PipelineQuery).mapTo[List[Pipeline]]
        future.map { resp =>
          ctx.complete(resp.toJson.toString)
        }
      }
    } ~
    path("pipeline" / "get") {
      parameter('pipeline_id.as[String]) { pipeline_id =>
        get { ctx =>
          val future = (api ? PipelineGet(pipeline_id)).mapTo[Try[Pipeline]]
          future.map {
            case Success(resp) => ctx.complete(resp.toJson.toString)
            case Failure(error) => ctx.complete(StatusCodes.NotFound, error.toString)
          }
        }
      }
    } ~
    path("pipeline" / "put") {
      parameter('pipeline_id.as[String], 'jar.as[String], 'main_class.as[String]) { (pipeline_id, jar, main_class) =>
        entity(as[PipelineConfig]) { config =>
          post { ctx =>
            val future = (api ? PipelinePut(pipeline_id, jar, main_class, config)).mapTo[Try[Boolean]]
            future.map {
              case Success(resp) => ctx.complete(Map[String, Boolean]("success" -> resp).toJson.toString)
              case Failure(error) => ctx.complete(StatusCodes.BadRequest, error.toString)
            }
          }
        }
      }
    } ~
    path("pipeline" / "update") {
      parameter('pipeline_id.as[String], 'active.as[Boolean]) { (pipeline_id, active) =>
        entity(as[Option[PipelineConfig]]) { configMaybe =>
          var config = configMaybe.orNull
          patch { ctx =>
            val state = if (active) PipelineState.RUNNING else PipelineState.STOPPED
            val future = (api ? PipelineUpdate(pipeline_id, state, config, _validate = true)).mapTo[Try[Boolean]]
            future.map {
              case Success(resp) => ctx.complete(Map[String, Boolean]("success" -> resp).toJson.toString)
              case Failure(error) => ctx.complete(StatusCodes.NotFound, error.toString)
            }
          }
        }
      }
    } ~
    path("pipeline" / "delete") {
      parameter('pipeline_id.as[String]) { pipeline_id =>
        delete { ctx =>
          val future = (api ? PipelineDelete(pipeline_id)).mapTo[Try[Boolean]]
          future.map {
            case Success(resp) => ctx.complete(Map[String, Boolean]("success" -> resp).toJson.toString)
            case Failure(error) => ctx.complete(StatusCodes.NotFound, error.toString)
          }
        }
      }
    }
  }
}
