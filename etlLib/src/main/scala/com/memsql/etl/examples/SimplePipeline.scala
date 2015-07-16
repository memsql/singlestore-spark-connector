package com.memsql.etl.examples

import java.text.SimpleDateFormat
import java.util.Calendar

import com.memsql.etl.api.Pipeline
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext, Time}

object SimplePipeline extends Pipeline[Long, String] {
  override def extract(ssc: StreamingContext): InputDStream[Long] = {
    new InputDStream[Long](ssc) {
      override def stop(): Unit = {}

      override def start(): Unit = {}

      override def compute(validTime: Time): Option[RDD[Long]] = {
        if (Math.random() > 0.7)
          Some(ssc.sparkContext.parallelize(Seq.fill(5)(Calendar.getInstance.getTimeInMillis)))
        else
          None
      }
    }
  }

  override def transform(from: DStream[Long]): DStream[String] = {
    val dateFormat = new SimpleDateFormat()
    from.map(dateFormat.format(_))
  }

  override def load(stream: DStream[String]): Unit = {}

  def main (args: Array[String]) {
      val sparkConf = new SparkConf()
                        .setMaster("spark://127.0.0.1:7077")

      val sparkStreamingContext = new StreamingContext(sparkConf, new Duration(5000))
      this.run(sparkStreamingContext)
  }
}
