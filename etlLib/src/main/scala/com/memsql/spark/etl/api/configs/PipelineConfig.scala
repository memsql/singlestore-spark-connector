package com.memsql.spark.etl.api.configs

import spray.json._
import DefaultJsonProtocol._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.util.Try

object PipelineConfigVersion {
  val CurrentPipelineConfigVersion = 1
}
import PipelineConfigVersion._

abstract class PhaseConfig
trait UserConfig {
  def class_name: String
  def value: JsValue

  def getConfigAsMap: Map[String, Any] = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(value.toString, classOf[Map[String, Any]])
  }

  // NOTE: we split the args to enforce that the path has at least one element
  def getConfigJsValue(pathHead: String, pathTail: String*): Option[JsValue] = getConfigJsValue(value, pathHead +: pathTail : _*)

  def getConfigString(pathHead: String, pathTail: String*): Option[String] = {
    Try(getConfigJsValue(value, pathHead +: pathTail: _*) match {
      // we have to handle this case separately from JsString
      case Some(JsNull) => Some(null)
      case default => default.map(_.convertTo[String])
    }).getOrElse(None)
  }

  def getConfigBoolean(pathHead: String, pathTail: String*): Option[Boolean] = {
    Try(getConfigJsValue(value, pathHead +: pathTail : _*).map(_.convertTo[Boolean])).getOrElse(None)
  }

  def getConfigInt(pathHead: String, pathTail: String*): Option[Int] = {
    Try(getConfigJsValue(value, pathHead +: pathTail : _*).map(_.convertTo[Int])).getOrElse(None)
  }

  def getConfigLong(pathHead: String, pathTail: String*): Option[Long] = {
    Try(getConfigJsValue(value, pathHead +: pathTail : _*).map(_.convertTo[Long])).getOrElse(None)
  }

  def getConfigFloat(pathHead: String, pathTail: String*): Option[Float] = {
    Try(getConfigJsValue(value, pathHead +: pathTail : _*).map(_.convertTo[Float])).getOrElse(None)
  }

  def getConfigDouble(pathHead: String, pathTail: String*): Option[Double] = {
    Try(getConfigJsValue(value, pathHead +: pathTail : _*).map(_.convertTo[Double])).getOrElse(None)
  }

  private def getConfigJsValue(jsValue: JsValue, path: String*): Option[JsValue] = {
    try {
      (path, jsValue) match {
        case (Seq(head), obj: JsObject) => Some(obj.fields(head))
        case (Seq(head), arr: JsArray) => Some(arr.elements(head.toInt))

        case (Seq(head, tail @ _*), obj: JsObject) => getConfigJsValue(obj.fields(head), tail: _*)
        case (Seq(head, tail @ _*), arr: JsArray) => getConfigJsValue(arr.elements(head.toInt), tail: _*)

        case default => None
      }
    } catch {
      case e: java.util.NoSuchElementException => None
      case e: java.lang.NumberFormatException => None
      case e: java.lang.NullPointerException => None
    }
  }
}

case class Phase[T](kind: T, config: JsValue)

object ExtractPhaseKind extends Enumeration {
  type ExtractPhaseKind = Value
  val Kafka, TestLines, User = Value
}
import ExtractPhaseKind._

object TransformPhaseKind extends Enumeration {
  type TransformPhaseKind = Value
  val Json, User = Value
}
import TransformPhaseKind._

object LoadPhaseKind extends Enumeration {
  type LoadPhaseKind = Value
  val MemSQL = Value
}
import LoadPhaseKind._

// The PipelineConfig object contains configuration for all phases of an
// ETL pipeline.
case class PipelineConfig(var extract: Phase[ExtractPhaseKind],
                          var transform: Phase[TransformPhaseKind],
                          var load: Phase[LoadPhaseKind],
                          var config_version: Int = PipelineConfigVersion.CurrentPipelineConfigVersion)
