package com.memsql.spark.etl.api

import java.util.Properties
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable.ListBuffer

import kafka.serializer.{DefaultDecoder, StringDecoder}
import kafka.consumer.{ConsumerConfig, Consumer}
import com.memsql.spark.etl.api.configs.{PhaseConfig, KafkaExtractConfig, KafkaExtractOutputType}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Time, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.InputDStream

class KafkaExtractor extends Extractor[(String, Any)] {
  private def bytesToString(in: Array[Byte]): String = {
    in match {
      case null => null
      case default => new String(default.map(_.toChar))
    }
  }

  /**
   *  This is a dumb extractor which reads directly from Kafka with a single thread without tracking any offsets
   * @param ssc
   * @param extractConfig
   * @return
   */
  def extract(ssc: StreamingContext, extractConfig: PhaseConfig): InputDStream[(String, Any)] = {
    val kafkaConfig = extractConfig.asInstanceOf[KafkaExtractConfig]

    val props = new Properties()
    props.put("zookeeper.connect", s"${kafkaConfig.host}:${kafkaConfig.port}")
    props.put("group.id", "1")
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")

    new InputDStream[(String, Any)](ssc) {
      private var consumerThread: Thread = null
      private val messageQueue = new ConcurrentLinkedQueue[(String, Array[Byte])]()

      private val connector = Consumer.create(new ConsumerConfig(props))
      private val streams = connector.createMessageStreams(Map(kafkaConfig.topic -> 1))
      private val topicStream = streams(kafkaConfig.topic)
      private val stream = topicStream(0)

      override def stop(): Unit = {
        connector.shutdown
        consumerThread.interrupt
        consumerThread.join
      }

      override def start(): Unit = {
        consumerThread = new Thread(new Runnable(){
          override def run(): Unit = {
            while (!Thread.currentThread.isInterrupted && stream.iterator.hasNext) {
              val message = stream.iterator.next
              messageQueue.add(Tuple2(bytesToString(message.key), message.message))
            }
          }
        })

        consumerThread.start
      }

      override def compute(validTime: Time): Option[RDD[(String, Any)]] = {
        messageQueue.size() match {
          case numMessages if numMessages > 0 => {
            var data = ListBuffer[(String, Any)]()
            for (i <- 1 to numMessages) {
              data += messageQueue.remove
            }

            kafkaConfig.output_type match {
              case Some(KafkaExtractOutputType.ByteArray) => Some(ssc.sparkContext.parallelize(data))
              case default => {
                Some(ssc.sparkContext.parallelize(data.map { x =>
                  Tuple2(x._1, bytesToString(x._2.asInstanceOf[Array[Byte]]))
                }))
              }
            }
          }
          case _ => None
        }
      }
    }
  }
}
