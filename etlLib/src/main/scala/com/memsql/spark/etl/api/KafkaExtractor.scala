package com.memsql.spark.etl.api

import kafka.serializer.{DefaultDecoder, StringDecoder}
import com.memsql.spark.etl.api.configs.{PhaseConfig, KafkaExtractConfig, KafkaExtractOutputType}
import org.apache.spark.streaming.StreamingContext
import com.memsql.spark.etl.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.InputDStream

class KafkaExtractor(consumerId: String) extends Extractor[(String, Any)] {
  def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchDuration: Long): InputDStream[(String, Any)] = {
    val kafkaConfig  = extractConfig.asInstanceOf[KafkaExtractConfig]
    val kafkaParams = Map(
      "metadata.broker.list" -> s"${kafkaConfig.host}:${kafkaConfig.port}"
      )
    val topicsSet = Set(kafkaConfig.topic)

    val createStreamFunc = kafkaConfig.output_type match {
      case Some(KafkaExtractOutputType.String) => KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](_: StreamingContext, _: Map[String, String], _: Set[String], _: Long)
      case default => KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](_: StreamingContext, _: Map[String, String], _: Set[String], _: Long)
    }

    createStreamFunc(ssc, kafkaParams, topicsSet, batchDuration).asInstanceOf[InputDStream[(String, Any)]]
  }
}
