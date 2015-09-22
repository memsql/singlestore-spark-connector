package com.memsql.spark.etl.api

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import spray.json._

import DefaultJsonProtocol._

import scala.util.Try

/**
 * Convenience methods for retrieving JSON values from a user defined configuration.
 */
trait UserConfig {
  def class_name: String // scalastyle:ignore
  def value: JsValue

  /**
   * Serializes the JSON configuration into a Map. Supports nested JSON. JSON objects are serialized into [[scala.collection.immutable.Map]]s and
   * JSON arrays are serialized into [[scala.List]]s.
   *
   * @return The JSON configuration serialized into a nested Map.
   */
  def getConfigAsMap: Map[String, Any] = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(value.toString, classOf[Map[String, Any]])
  }

  /**
   * Retrieve a [[spray.json.JsValue]] at the specified path from the user defined configuration.
   *
   * @param pathHead Key in the user defined object
   * @param pathTail Additional nested keys
   * @return An [[scala.Option]] containing the [[spray.json.JsValue]], or [[scala.None]] if the path is invalid.
   */
  // NOTE: we split the args to enforce that the path has at least one element
  def getConfigJsValue(pathHead: String, pathTail: String*): Option[JsValue] = getConfigJsValue(value, pathHead +: pathTail : _*)

  /**
   * Retrieve a [[scala.Predef.String]] at the specified path from the user defined configuration.
   *
   * @param pathHead Key in the user defined object
   * @param pathTail Additional nested keys
   * @return An [[scala.Option]] containing the [[scala.Predef.String]], or [[scala.None]] if the path is invalid.
   */
  def getConfigString(pathHead: String, pathTail: String*): Option[String] = {
    Try(getConfigJsValue(value, pathHead +: pathTail: _*) match {
      // we have to handle this case separately from JsString
      case Some(JsNull) => Some(null)
      case default => default.map(_.convertTo[String])
    }).getOrElse(None)
  }

  /**
   * Retrieve a [[scala.Boolean]] at the specified path from the user defined configuration.
   *
   * @param pathHead Key in the user defined object
   * @param pathTail Additional nested keys
   * @return An [[scala.Option]] containing the [[scala.Boolean]], or [[scala.None]] if the path is invalid.
   */
  def getConfigBoolean(pathHead: String, pathTail: String*): Option[Boolean] = {
    Try(getConfigJsValue(value, pathHead +: pathTail : _*).map(_.convertTo[Boolean])).getOrElse(None)
  }

  /**
   * Retrieve an [[scala.Int]] at the specified path from the user defined configuration.
   *
   * @param pathHead Key in the user defined object
   * @param pathTail Additional nested keys
   * @return An [[scala.Option]] containing the [[scala.Int]], or [[scala.None]] if the path is invalid.
   */
  def getConfigInt(pathHead: String, pathTail: String*): Option[Int] = {
    Try(getConfigJsValue(value, pathHead +: pathTail : _*).map(_.convertTo[Int])).getOrElse(None)
  }

  /**
   * Retrieve a [[scala.Long]] at the specified path from the user defined configuration.
   *
   * @param pathHead Key in the user defined object
   * @param pathTail Additional nested keys
   * @return An [[scala.Option]] containing the [[scala.Long]], or [[scala.None]] if the path is invalid.
   */
  def getConfigLong(pathHead: String, pathTail: String*): Option[Long] = {
    Try(getConfigJsValue(value, pathHead +: pathTail : _*).map(_.convertTo[Long])).getOrElse(None)
  }

  /**
   * Retrieve a [[scala.Float]] at the specified path from the user defined configuration.
   *
   * @param pathHead Key in the user defined object
   * @param pathTail Additional nested keys
   * @return An [[scala.Option]] containing the [[scala.Float]], or [[scala.None]] if the path is invalid.
   */
  def getConfigFloat(pathHead: String, pathTail: String*): Option[Float] = {
    Try(getConfigJsValue(value, pathHead +: pathTail : _*).map(_.convertTo[Float])).getOrElse(None)
  }

  /**
   * Retrieve a [[scala.Double]] at the specified path from the user defined configuration.
   *
   * @param pathHead Key in the user defined object
   * @param pathTail Additional nested keys
   * @return An [[scala.Option]] containing the [[scala.Double]], or [[scala.None]] if the path is invalid.
   */
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
