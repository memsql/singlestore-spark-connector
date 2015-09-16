package com.memsql.spark.etl.api

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.MemSQLKafkaUtils
import org.apache.spark.streaming.dstream.InputDStream
import com.memsql.spark.etl.api.configs.{PhaseConfig, KafkaExtractConfig}
import com.memsql.spark.etl.utils.PhaseLogger

class KafkaExtractor(consumerId: String) extends ByteArrayExtractor {
  def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchDuration: Long, logger: PhaseLogger): InputDStream[Array[Byte]] = {
    val kafkaConfig  = extractConfig.asInstanceOf[KafkaExtractConfig]
    val kafkaParams = Map(
      "metadata.broker.list" -> s"${kafkaConfig.host}:${kafkaConfig.port}"
    )
    val topics = Set(kafkaConfig.topic)

    MemSQLKafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topics, batchDuration)
  }
}
