package com.memsql.spark.etl.api

import com.memsql.spark.etl.api.configs.PhaseConfig
import scala.reflect.ClassTag
import kafka.serializer.Decoder
import org.apache.spark.streaming.StreamingContext
import kafka.message.MessageAndMetadata
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.InputDStream


object KafkaExtractor extends Serializable {

  def directKafkaExtractor[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag]
  (
    kafkaParams: Map[String, String],
    topics: Set[String]
    ): Extractor[(K, V)] = {
    new Extractor[(K, V)] {
      override def extract(ssc: StreamingContext, extractConfig: PhaseConfig): InputDStream[(K, V)] = KafkaUtils.createDirectStream[K, V, KD, VD](ssc, kafkaParams, topics)
    }
  }

  def directKafkaExtractor[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag, R: ClassTag]
  (
    kafkaParams: Map[String, String],
    fromOffsets: Map[TopicAndPartition, Long],
    messageHandler: MessageAndMetadata[K, V] => R
    ): Extractor[R] = {
    new Extractor[R] {
      override def extract(ssc: StreamingContext, extractConfig: PhaseConfig): InputDStream[R] = KafkaUtils.createDirectStream[K, V, KD, VD, R](ssc, kafkaParams, fromOffsets, messageHandler)
    }
  }
}
