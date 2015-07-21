package com.memsql.spark.examples

import java.text.SimpleDateFormat
import java.util.Calendar

import com.memsql.spark.connector._
import com.memsql.spark.etl.api.ETLPipeline
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{StreamingContext, Time}

case class MemSQLETLApp() extends ETLPipeline[Long,Array[String]]{
  def extract(ssc: StreamingContext): InputDStream[Long] = {
    new InputDStream[Long](ssc) {
      override def stop(): Unit = {}

      override def start(): Unit = {}

      override def compute(validTime: Time): Option[RDD[Long]] = {
        Some(ssc.sparkContext.parallelize(Seq.fill(5)(Calendar.getInstance.getTimeInMillis)))
      }
    }
  }

  def transform(from: DStream[Long]): DStream[Array[String]] = {
    val dateFormat = new SimpleDateFormat()
    from.map { x =>
      Array(dateFormat.format(x))
    }
  }

  def load(stream: DStream[Array[String]]): Unit = {
    stream.foreachRDD { rdd =>
      rdd.saveToMemsql("127.0.0.1", 3306, "root", "", "test", "test")
    }
  }
}
