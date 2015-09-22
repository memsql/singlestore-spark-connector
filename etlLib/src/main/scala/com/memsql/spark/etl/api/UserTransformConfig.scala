package com.memsql.spark.etl.api

import spray.json.JsValue

/**
 * User defined configuration passed [[com.memsql.spark.etl.api.SimpleByteArrayTransformer]].
 *
 * @param class_name The fully qualified class name for the requested Transformer.
 * @param value The JSON configuration passed from MemSQL Ops as a [[spray.json.JsValue]].
 */
case class UserTransformConfig(class_name:String, value: JsValue) extends PhaseConfig with UserConfig
