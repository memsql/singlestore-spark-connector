package com.memsql.spark.etl.api

import com.fasterxml.jackson.databind.{ObjectMapper, JsonMappingException}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.memsql.spark.etl.utils.Logging

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import com.memsql.spark.etl.api.configs.PhaseConfig
import com.memsql.spark.etl.api.configs.UserTransformConfig

trait TransformerConfiguredByJSON[S] extends Transformer[S] with Logging {
  override def transform(sqlContext: SQLContext, rdd: RDD[S], config: PhaseConfig): DataFrame = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val configMapString = config.asInstanceOf[UserTransformConfig].value
    var configMap: Map[String, String] = Map[String, String]()

    try {
      configMap = mapper.readValue(configMapString, classOf[Map[String, String]])
    } catch {
      case e: JsonMappingException => {
        logWarn(s"Couldn't parse JSON config, using empty Map: $configMapString", e)
      }
    }

    transform(rdd, sqlContext, configMap)
  }
  
  def transform(rdd: RDD[S], sqlContext: SQLContext, config: Map[String,String]): DataFrame
}
