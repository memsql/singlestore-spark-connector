package com.memsql.spark.etl.api

import kafka.serializer.{DefaultDecoder, StringDecoder}
import com.memsql.spark.etl.api.configs.{PhaseConfig, KafkaExtractConfig}
import org.apache.spark.streaming.StreamingContext
import com.memsql.spark.etl.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.InputDStream

class KafkaExtractor(consumerId: String) extends ByteArrayExtractor {
  def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchDuration: Long): InputDStream[Array[Byte]] = {
    val kafkaConfig  = extractConfig.asInstanceOf[KafkaExtractConfig]
    val kafkaParams = Map(
      "metadata.broker.list" -> s"${kafkaConfig.host}:${kafkaConfig.port}"
      )
    val topicsSet = Set(kafkaConfig.topic)

    KafkaUtils.createDirectValueStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet, batchDuration)
  }
}
