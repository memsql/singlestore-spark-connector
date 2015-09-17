package com.memsql.spark.phases

import com.memsql.spark.etl.api.{ByteArrayExtractor, PhaseConfig}
import com.memsql.spark.etl.utils.PhaseLogger
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.MemSQLKafkaUtils

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
