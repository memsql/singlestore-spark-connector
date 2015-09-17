package com.memsql.spark.etl.utils

import spray.json._

class JsonEnumProtocol extends DefaultJsonProtocol {
  def jsonEnum[T <: Enumeration](enu: T) = new JsonFormat[T#Value] {
    def write(obj: T#Value) = JsString(obj.toString)

    def read(json: JsValue) = json match {
      case JsString(txt) => {
        try {
          enu.withName(txt)
        } catch {
          case e: NoSuchElementException => deserializationError(s"expected a value from $enu instead of $txt")
        }
      }
      case default => deserializationError(s"$enu value expected")
    }
  }
}
