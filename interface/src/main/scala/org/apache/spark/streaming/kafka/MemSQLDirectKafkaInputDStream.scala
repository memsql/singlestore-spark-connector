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

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.streaming.{Time, StreamingContext}

import scala.reflect.ClassTag

class MemSQLDirectKafkaInputDStream[K: ClassTag, V: ClassTag, U <: Decoder[K]: ClassTag, T <: Decoder[V]: ClassTag, R: ClassTag](
  @transient ssc_ : StreamingContext,
  override val kafkaParams: Map[String, String],
  override val fromOffsets: Map[TopicAndPartition, Long],
  messageHandler: MessageAndMetadata[K, V] => R,
  batchDuration: Long) extends DirectKafkaInputDStream[K, V, U, T, R](ssc_, kafkaParams, fromOffsets, messageHandler) {

  override val checkpointData = null

  //NOTE: We override this to use the pipeline specific batch duration
  override val maxMessagesPerPartition: Option[Long] = {
    val ratePerSec = context.sparkContext.getConf.getInt(
      "spark.streaming.kafka.maxRatePerPartition", 0)
    if (ratePerSec > 0) {
      val secsPerBatch = batchDuration / 1000
      Some(secsPerBatch * ratePerSec)
    } else {
      None
    }
  }

  //Track the previous batch's offsets so we can retry the batch if it fails
  var prevOffsets: Map[TopicAndPartition, Long] = null

  //NOTE: We override this to suppress input info tracking because the StreamingContext has not been started.
  override def compute(validTime: Time): Option[KafkaRDD[K, V, U, T, R]] = {
    val untilOffsets = clamp(latestLeaderOffsets(maxRetries))
    val rdd = KafkaRDD[K, V, U, T, R](
      context.sparkContext, kafkaParams, currentOffsets, untilOffsets, messageHandler)

    // The vanilla implementation calls the inputInfoTracker here. This code is left as a comment here for clarity.
    /*
     * Report the record number of this batch interval to InputInfoTracker.
     * val numRecords = rdd.offsetRanges.map(r => r.untilOffset - r.fromOffset).sum
     * val inputInfo = InputInfo(id, numRecords)
     * ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
     */

    prevOffsets = currentOffsets
    currentOffsets = untilOffsets.map(kv => kv._1 -> kv._2.offset)

    prevOffsets == currentOffsets match {
      case false => Some(rdd)
      case true => None
    }
  }

  def getCurrentOffsets(): Map[TopicAndPartition, Long] = currentOffsets
  def setCurrentOffsets(offsets: Map[TopicAndPartition, Long]): Unit = {
    currentOffsets = offsets
  }
}
