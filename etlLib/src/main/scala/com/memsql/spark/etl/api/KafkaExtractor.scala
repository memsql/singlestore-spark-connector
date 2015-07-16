package com.memsql.spark.etl.api

import scala.reflect.ClassTag
import kafka.serializer.Decoder
import org.apache.spark.streaming.StreamingContext
import kafka.message.MessageAndMetadata
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.api.java.{JavaStreamingContext, JavaInputDStream}
import org.apache.spark.streaming.dstream.InputDStream
import java.lang.{Integer => JInt}
import java.lang.{Long => JLong}
import java.util.{Map => JMap}
import java.util.{Set => JSet}
import org.apache.spark.api.java.function.{Function => JFunction}


object KafkaExtractor extends Serializable {

  def directKafkaExtractor[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag]
  (
    kafkaParams: Map[String, String],
    topics: Set[String]
    ): Extractor[(K, V)] = {
    new Extractor[(K, V)] {
      override def extract(ssc: StreamingContext): InputDStream[(K, V)] = KafkaUtils.createDirectStream[K, V, KD, VD](ssc, kafkaParams, topics)
    }
  }

  def directKafkaExtractor[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag, R: ClassTag]
  (
    kafkaParams: Map[String, String],
    fromOffsets: Map[TopicAndPartition, Long],
    messageHandler: MessageAndMetadata[K, V] => R
    ): Extractor[R] = {
    new Extractor[R] {
      override def extract(ssc: StreamingContext): InputDStream[R] = KafkaUtils.createDirectStream[K, V, KD, VD, R](ssc, kafkaParams, fromOffsets, messageHandler)
    }
  }

  def directKafkaExtractor[K, V, KD <: Decoder[K], VD <: Decoder[V], R]
  (
    keyClass: Class[K],
    valueClass: Class[V],
    keyDecoderClass: Class[KD],
    valueDecoderClass: Class[VD],
    recordClass: Class[R],
    kafkaParams: JMap[String, String],
    fromOffsets: JMap[TopicAndPartition, JLong],
    messageHandler: JFunction[MessageAndMetadata[K, V], R]
    ): JavaExtractor[R] = {
    new JavaExtractor[R] {
      override def extract(ssc: JavaStreamingContext): JavaInputDStream[R] = KafkaUtils.createDirectStream(ssc, keyClass, valueClass, keyDecoderClass, valueDecoderClass, recordClass, kafkaParams, fromOffsets, messageHandler)
    }
  }

  def directKafkaExtractor[K, V, KD <: Decoder[K], VD <: Decoder[V]]
  (
    keyClass: Class[K],
    valueClass: Class[V],
    keyDecoderClass: Class[KD],
    valueDecoderClass: Class[VD],
    kafkaParams: JMap[String, String],
    topics: JSet[String]
    ): JavaExtractor[(K, V)] = {
    new JavaExtractor[(K, V)] {
      override def extract(ssc: JavaStreamingContext): JavaInputDStream[(K, V)] = JavaInputDStream.fromInputDStream(KafkaUtils.createDirectStream(ssc, keyClass, valueClass, keyDecoderClass, valueDecoderClass, kafkaParams, topics).inputDStream)
    }
  }

}
