package org.apache.spark.streaming.kafka

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified for MemSQL Streamliner
 */

import com.memsql.spark.etl.utils.Logging
import kafka.common.TopicAndPartition
import kafka.utils.{ZkUtils, ZKStringSerializer}
import org.I0Itec.zkclient.ZkClient

import scala.reflect.ClassTag

import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder

import org.apache.spark.SparkException
import org.apache.spark.annotation.Experimental
import org.apache.spark.streaming.StreamingContext

import scala.util.control.NonFatal

class KafkaException(message: String) extends Exception(message)

object MemSQLKafkaUtils extends Logging {
  val ZK_SESSION_TIMEOUT = Int.MaxValue //milliseconds
  val ZK_CONNECT_TIMEOUT = 10000 //milliseconds

  /**
   * :: Experimental ::
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   * NOTE: Modified to use Zookeeper quorum for MemSQL Streamliner.
   *
   * Points to note:
   *  - No receivers: This stream does not use any receiver. It directly queries Kafka
   *  - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   *    by the stream itself. For interoperability with Kafka monitoring tools that depend on
   *    Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
   *    You can access the offsets used in each batch from the generated RDDs (see
   *    [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
   *  - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   *    in the [[StreamingContext]]. The information on consumed offset can be
   *    recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   *  - End-to-end semantics: This stream ensures that every records is effectively received and
   *    transformed exactly once, but gives no guarantees on whether the transformed data are
   *    outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   *    that the output operation is idempotent, or use transactions to output records atomically.
   *    See the programming guide for more details.
   *
   * @param ssc StreamingContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *   configuration parameters</a>. Requires "memsql.zookeeper.connect" to be set with Zookeeper servers,
   *   specified in host1:port1,host2:port2/chroot2 form.
   *   If not starting from a checkpoint, "auto.offset.reset" may be set to "largest" or "smallest"
   *   to determine where the stream starts (defaults to "largest")
   * @param topics Names of the topics to consume
   * @param batchInterval Batch interval for this pipeline. NOTE: Modified for MemSQL Streamliner
   * @param lastCheckpoint Offsets to use when initializing the consumer. If the topic, partition count,
   *   or offsets from the checkpoint are invalid, fall back to the offsets specified by "auto.offset.reset".
   *   NOTE: Modified for MemSQL Streamliner
   *
   */
  @Experimental
  def createDirectStreamFromZookeeper[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag] (
                                      ssc: StreamingContext,
                                      kafkaParams: Map[String, String],
                                      topics: Set[String],
                                      batchInterval: Long,
                                      lastCheckpoint: Option[Map[String, Any]]): MemSQLDirectKafkaInputDStream[K, V, KD, VD, V] = {
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => mmd.message
    val brokers = getKafkaBrokersFromZookeeper(kafkaParams)
    val kafkaParamsWithBrokers = kafkaParams + ("metadata.broker.list" -> brokers.mkString(","))

    val initialOffsets = getInitialOffsetsFromZookeeper(kafkaParamsWithBrokers, topics, lastCheckpoint)
    new MemSQLDirectKafkaInputDStream[K, V, KD, VD, V](
      ssc, kafkaParamsWithBrokers, initialOffsets, messageHandler, batchInterval)
  }

  /**
   * :: Experimental ::
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   *
   * Points to note:
   *  - No receivers: This stream does not use any receiver. It directly queries Kafka
   *  - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   *    by the stream itself. For interoperability with Kafka monitoring tools that depend on
   *    Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
   *    You can access the offsets used in each batch from the generated RDDs (see
   *    [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
   *  - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   *    in the [[StreamingContext]]. The information on consumed offset can be
   *    recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   *  - End-to-end semantics: This stream ensures that every records is effectively received and
   *    transformed exactly once, but gives no guarantees on whether the transformed data are
   *    outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   *    that the output operation is idempotent, or use transactions to output records atomically.
   *    See the programming guide for more details.
   *
   * @param ssc StreamingContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *   configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *   to be set with Kafka broker(s) (NOT zookeeper servers), specified in
   *   host1:port1,host2:port2 form.
   *   If not starting from a checkpoint, "auto.offset.reset" may be set to "largest" or "smallest"
   *   to determine where the stream starts (defaults to "largest")
   * @param topics Names of the topics to consume
   * @param batchInterval Batch interval for this pipeline. NOTE: Modified for MemSQL Streamliner
   * @param lastCheckpoint Offsets to use when initializing the consumer. If the topic, partition count,
   *   or offsets from the checkpoint are invalid, fall back to the offsets specified by "auto.offset.reset".
   *   NOTE: Modified for MemSQL Streamliner
   */
  @Experimental
  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag] (
                         ssc: StreamingContext,
                         kafkaParams: Map[String, String],
                         topics: Set[String],
                         batchInterval: Long,
                         lastCheckpoint: Option[Map[String, Any]]): MemSQLDirectKafkaInputDStream[K, V, KD, VD, V] = {
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => mmd.message
    val initialOffsets = getInitialOffsets(kafkaParams, topics, lastCheckpoint)
    new MemSQLDirectKafkaInputDStream[K, V, KD, VD, V](
      ssc, kafkaParams, initialOffsets, messageHandler, batchInterval)
  }

  private def getInitialOffsets(kafkaParams: Map[String, String], topics: Set[String],
                                lastCheckpoint: Option[Map[String, Any]]): Map[TopicAndPartition, Long] = {
    val kc = new KafkaCluster(kafkaParams)
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
    val broker = kafkaParams("metadata.broker.list")

    val checkpointBroker = lastCheckpoint.flatMap { x => x.get("broker") }
    val checkpointOffsets = lastCheckpoint.flatMap(getCheckpointOffsets)
    val zookeeperOffsets = getZookeeperOffsets(kc, topics, reset)

    (checkpointBroker, checkpointOffsets, zookeeperOffsets) match {
      case (None, _, _) => zookeeperOffsets
      case (_, None, _) => zookeeperOffsets
      case (Some(checkpointBroker), Some(offsets), _) => {
        if (checkpointBroker != broker) {
          logWarn("Kafka broker has changed since the last checkpoint, falling back to Zookeeper offsets")
          zookeeperOffsets
        } else if (offsets.size != zookeeperOffsets.size) {
          logWarn("Kafka partition count has changed since the last checkpoint, falling back to Zookeeper offsets")
          zookeeperOffsets
        } else if (offsets.nonEmpty && offsets.keys.head.topic != zookeeperOffsets.keys.head.topic) {
          logWarn("Kafka topic has changed since the last checkpoint, falling back to Zookeeper offsets")
          zookeeperOffsets
        } else {
          offsets
        }
      }
    }
  }

  private def getKafkaBrokersFromZookeeper(kafkaParams: Map[String, String]): Seq[String] = {
    val zkServerString = kafkaParams("memsql.zookeeper.connect")
    val zkClient = new ZkClient(zkServerString, ZK_SESSION_TIMEOUT, ZK_CONNECT_TIMEOUT, ZKStringSerializer)
    ZkUtils.getAllBrokersInCluster(zkClient).map { b => s"${b.host}:${b.port}" }.sorted
  }

  private def getInitialOffsetsFromZookeeper(kafkaParams: Map[String, String], topics: Set[String],
                                lastCheckpoint: Option[Map[String, Any]]): Map[TopicAndPartition, Long] = {
    val zkServerString = kafkaParams("memsql.zookeeper.connect")
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)

    val kc = new KafkaCluster(kafkaParams)

    val checkpointZkServers = lastCheckpoint.flatMap { x => x.get("zookeeper") }.flatMap {
      case x: String => Some(x.split(",").sorted.mkString(","))
      case default => None
    }
    val checkpointOffsets = lastCheckpoint.flatMap(getCheckpointOffsets)
    val zookeeperOffsets = getZookeeperOffsets(kc, topics, reset)

    (checkpointZkServers, checkpointOffsets, zookeeperOffsets) match {
      case (None, _, _) => zookeeperOffsets
      case (_, None, _) => zookeeperOffsets
      case (Some(checkpointZkServerString), Some(offsets), _) => {
        if (checkpointZkServerString != zkServerString) {
          logWarn("Zookeeper quorum list for this extractor has changed since the last checkpoint, falling back to default offsets")
          zookeeperOffsets
        } else if (offsets.size != zookeeperOffsets.size) {
          logWarn("Kafka partition count has changed since the last checkpoint, falling back to Zookeeper offsets")
          zookeeperOffsets
        } else if (offsets.nonEmpty && offsets.keys.head.topic != zookeeperOffsets.keys.head.topic) {
          logWarn("Kafka topic has changed since the last checkpoint, falling back to Zookeeper offsets")
          zookeeperOffsets
        } else {
          offsets
        }
      }
    }
  }

  // Serializes the checkpoint data into the format expected by KafkaDirectInputDStream
  private def getCheckpointOffsets(checkpoint: Map[String, Any]): Option[Map[TopicAndPartition, Long]] = {
    try {
      val offsets = checkpoint("offsets").asInstanceOf[List[Map[String, Any]]]
      val checkpointOffsets = offsets.map { partitionInfo =>
        val topic = partitionInfo("topic").asInstanceOf[String]
        val partition = partitionInfo("partition").asInstanceOf[Int]
        val offset = partitionInfo("offset").asInstanceOf[Number].longValue

        (TopicAndPartition(topic, partition), offset)
      }.toMap
      Some(checkpointOffsets)
    } catch {
      case NonFatal(e) => {
        logWarn("Kafka checkpoint data is invalid, it will be ignored", e)
        None
      }
    }
  }

  private def getZookeeperOffsets(kc: KafkaCluster, topics: Set[String], reset: Option[String]): Map[TopicAndPartition, Long] = {
    (for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (if (reset == Some("smallest")) {
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)
      }).right
    } yield {
      leaderOffsets.map { case (tp, lo) =>
        (tp, lo.offset)
      }
    }).fold(
      errs => {
        val wrappedErrs = errs.map {
          case err: java.nio.channels.ClosedChannelException => {
            val broker = kc.config.seedBrokers.toList(0)
            new KafkaException(s"Could not connect to Kafka broker(s) at ${broker._1}:${broker._2}: $err")
          }
          case default => default
        }
        throw new SparkException(wrappedErrs.mkString("\n"))
      },
      ok => ok
    )
  }
}
