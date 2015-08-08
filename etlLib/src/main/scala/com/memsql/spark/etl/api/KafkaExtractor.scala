package com.memsql.spark.etl.api

import kafka.serializer.{DefaultDecoder, StringDecoder}
import com.memsql.spark.etl.api.configs.{PhaseConfig, KafkaExtractConfig, KafkaExtractOutputType}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.InputDStream

class KafkaExtractor extends Extractor[(String, Any)] {
  override def extract(ssc: StreamingContext, extractConfig: PhaseConfig): InputDStream[(String, Any)] = {
    val kafkaConfig = extractConfig.asInstanceOf[KafkaExtractConfig]
    val kafkaParams = Map(
      "metadata.broker.list" -> s"${kafkaConfig.host}:${kafkaConfig.port}"
    )
    val topicsSet = Set(kafkaConfig.topic)
    val createStreamFunc = kafkaConfig.output_type match {
      case Some(KafkaExtractOutputType.String) => KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](_: StreamingContext, _: Map[String, String], _: Set[String])
      case default => KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](_: StreamingContext, _: Map[String, String], _: Set[String])
    }
    createStreamFunc(ssc, kafkaParams, topicsSet).asInstanceOf[InputDStream[(String, Any)]]
  }
}
