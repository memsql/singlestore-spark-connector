package com.memsql.spark.etl.api

import com.fasterxml.jackson.databind.{ObjectMapper, JsonMappingException}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.memsql.spark.etl.utils.Logging

import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import com.memsql.spark.etl.api.configs.PhaseConfig
import com.memsql.spark.etl.api.configs.UserTransformConfig

trait TransformerConfiguredByJSON extends ByteArrayTransformer with Logging {
  private var configMap: Map[String, String] = null

  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: PhaseConfig, logger: Logger): DataFrame = {
    if (configMap == null) {
      val configMapString = config.asInstanceOf[UserTransformConfig].value

      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)

      configMap = Map[String, String]()
      try {
        configMap = mapper.readValue(configMapString, classOf[Map[String, String]])
      } catch {
        case e: JsonMappingException => {
          logWarn(s"Couldn't parse JSON config, using empty config: $configMapString", e)
        }
      }
    }

    transform(rdd, sqlContext, configMap, logger)
  }

  def transform(rdd: RDD[Array[Byte]], sqlContext: SQLContext, config: Map[String,String], logger: Logger): DataFrame
}
