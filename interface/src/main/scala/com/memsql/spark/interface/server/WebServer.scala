package com.memsql.spark.interface.server

import akka.actor.{ActorContext, Actor}
import akka.event.Logging._
import akka.pattern.ask
import com.memsql.spark.interface._
import com.memsql.spark.interface.api._
import com.memsql.spark.etl.api.configs.PipelineConfig
import spray.http.{HttpRequest, HttpResponse, StatusCodes}
import spray.httpx.SprayJsonSupport._
import spray.routing.{Route, Directive0, HttpService}
import spray.json._
import spray.routing.directives.LogEntry
import scala.concurrent.duration._
import akka.util.Timeout
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure, Try}

class WebServer(val appWebUIPort: Int) extends Actor with WebService {
  def actorRefFactory: ActorContext = context
  def receive: Receive = runRoute(routeWithLogging)
}

trait WebService extends HttpService {
  import com.memsql.spark.interface.api.ApiJsonProtocol._
  import com.memsql.spark.interface.api.ApiActor._
  import ooyala.common.akka.web.JsonUtils._

  val appWebUIPort: Int
  val api = actorRefFactory.actorSelection("/user/api")
  implicit val timeout = Timeout(5.seconds)

  // logs just the request method and response status at debug level
  def requestMethodAndResponseStatusAsDebug(req: HttpRequest): Any => Option[LogEntry] = {
    case res: HttpResponse => Some(LogEntry(req.method + ":" + req.uri + "," + req.entity + " - " + res.message.status + ":" + res.message.message, DebugLevel))
    case _ => None // other kind of responses
  }

  def routeWithLogging: Route = logRequestResponse(requestMethodAndResponseStatusAsDebug _)(route)

  val route = {
    path("version") {
      get {
        complete(JsObject("name" -> JsString("MemSQL Spark Interface"), "version" -> JsString(Main.VERSION)))
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
    path("webui_port") {
      get {
        complete(JsObject("webui_port" -> JsNumber(appWebUIPort)))
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
      parameter('pipeline_id.as[String], 'batch_interval.as[Long]) { (pipeline_id, batch_interval) =>
        entity(as[PipelineConfig]) { config =>
          post { ctx =>
            val future = (api ? PipelinePut(pipeline_id, batch_interval, config)).mapTo[Try[Boolean]]
            future.map {
              case Success(resp) => ctx.complete(Map[String, Boolean]("success" -> resp).toJson.toString)
              case Failure(error) => ctx.complete(StatusCodes.BadRequest, error.toString)
            }
          }
        }
      }
    } ~
    path("pipeline" / "update") {
      parameter('pipeline_id.as[String], 'active.as[Boolean].?, 'batch_interval.as[Long].?, 'trace_batch_count.as[Int].?) {
        (pipeline_id, active, batch_interval, trace_batch_count) =>
        entity(as[Option[PipelineConfig]]) { configMaybe =>
          patch { ctx =>
            val state = active match {
              case Some(true) => Some(PipelineState.RUNNING)
              case Some(false) => Some(PipelineState.STOPPED)
              case default => None
            }
            val error = state match {
              case Some(PipelineState.RUNNING) => Some("")
              case default => None
            }
            val future = (api ? PipelineUpdate(pipeline_id, state, batch_interval, configMaybe, error=error,
                                               trace_batch_count=trace_batch_count, _validate = true)).mapTo[Try[Boolean]]
            future.map {
              case Success(resp) => ctx.complete(Map[String, Boolean]("success" -> resp).toJson.toString)
              case Failure(error) => ctx.complete(StatusCodes.NotFound, error.toString)
            }
          }
        }
      }
    } ~
    path("pipeline" / "metrics") {
      parameter('pipeline_id.as[String], 'last_timestamp.as[Long].?) { (pipeline_id, last_timestamp) =>
        get { ctx =>
          val future = (api ? PipelineMetrics(pipeline_id, last_timestamp)).mapTo[Try[List[PipelineMetricRecord]]]
          future.map {
            case Success(resp) => ctx.complete(resp.toJson.toString)
            case Failure(error) => ctx.complete(StatusCodes.NotFound, error.toString)
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
