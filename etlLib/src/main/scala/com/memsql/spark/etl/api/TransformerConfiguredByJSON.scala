package com.memsql.spark.etl.api

import com.memsql.spark.etl.api.configs.PhaseConfig
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.`type`.TypeReference;

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import com.memsql.spark.etl.api.configs.PhaseConfig
import com.memsql.spark.etl.api.configs.UserTransformConfig


trait TransformerConfiguredByJSON[S] extends Transformer[S] {
  override def transform(sqlContext: SQLContext, rdd: RDD[S], config: PhaseConfig): DataFrame = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val configMap: Map[String,String] = mapper.readValue(config.asInstanceOf[UserTransformConfig].value, classOf[Map[String,String]])
    transform(rdd, sqlContext, configMap)
  }
  
  def transform(rdd: RDD[S], sqlContext: SQLContext, config: Map[String,String]): DataFrame
}
