package com.memsql.spark.phases

import com.memsql.spark.etl.api.{ByteArrayExtractor, PhaseConfig}
import com.memsql.spark.etl.utils.PhaseLogger
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.{MemSQLDirectKafkaInputDStream, MemSQLKafkaUtils}
import org.apache.spark.streaming.dstream.InputDStream

// A Kafka extractor configured by a Zookeeper quorum.
case class ZookeeperManagedKafkaExtractConfig(zk_quorum: List[String], topic: String) extends PhaseConfig

class ZookeeperManagedKafkaExtractor extends ByteArrayExtractor {
  var CHECKPOINT_DATA_VERSION = 1

  var dstream: MemSQLDirectKafkaInputDStream[String, Array[Byte], StringDecoder, DefaultDecoder, Array[Byte]] = null
  var zkQuorum: String = null

  def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchDuration: Long, logger: PhaseLogger): InputDStream[Array[Byte]] = {
    val kafkaConfig  = extractConfig.asInstanceOf[ZookeeperManagedKafkaExtractConfig]
    zkQuorum = kafkaConfig.zk_quorum.sorted.mkString(",")
    val kafkaParams = Map[String, String](
      "memsql.zookeeper.connect" -> zkQuorum
    )
    val topics = Set(kafkaConfig.topic)

    dstream = MemSQLKafkaUtils.createDirectStreamFromZookeeper[String, Array[Byte], StringDecoder, DefaultDecoder](
      ssc, kafkaParams, topics, batchDuration, lastCheckpoint)
    dstream
  }

  override def batchCheckpoint: Option[Map[String, Any]] = {
    dstream match {
      case null => None
      case default => {
        val currentOffsets = dstream.getCurrentOffsets.map { case (tp, offset) =>
          Map("topic" -> tp.topic, "partition" -> tp.partition, "offset" -> offset)
        }
        Some(Map("offsets" -> currentOffsets, "zookeeper" -> zkQuorum, "version" -> CHECKPOINT_DATA_VERSION))
      }
    }
  }

  override def batchRetry: Unit = {
    if (dstream.prevOffsets != null) {
      dstream.setCurrentOffsets(dstream.prevOffsets)
    }
  }
}

// NOTE: this is the original Kafka extractor which required a single Kafka broker in its config.
// It is replaced by the above extractor and config.
case class KafkaExtractConfig(host: String, port: Int, topic: String) extends PhaseConfig

class KafkaExtractor extends ByteArrayExtractor {
  var dstream: MemSQLDirectKafkaInputDStream[String, Array[Byte], StringDecoder, DefaultDecoder, Array[Byte]] = null
  var broker: String = null

  def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchDuration: Long, logger: PhaseLogger): InputDStream[Array[Byte]] = {
    val kafkaConfig  = extractConfig.asInstanceOf[KafkaExtractConfig]
    broker = s"${kafkaConfig.host}:${kafkaConfig.port}"
    val kafkaParams = Map(
      "metadata.broker.list" -> broker
    )
    val topics = Set(kafkaConfig.topic)

    dstream = MemSQLKafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topics, batchDuration, lastCheckpoint)
    dstream
  }
}
