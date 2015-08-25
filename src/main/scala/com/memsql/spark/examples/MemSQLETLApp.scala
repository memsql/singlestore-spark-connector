package com.memsql.spark.examples

import java.text.SimpleDateFormat
import java.util.Calendar

import com.memsql.spark.etl.api._
import com.memsql.spark.etl.api.configs._
import com.memsql.spark.etl.utils.ByteUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

class MemSQLExtractor extends ByteArrayExtractor {
  override def extract(ssc: StreamingContext, config: PhaseConfig, batchInterval: Long): InputDStream[Array[Byte]] = {
    new InputDStream[Array[Byte]](ssc) {
      override def stop(): Unit = {}

      override def start(): Unit = {}

      override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
        Some(ssc.sparkContext.parallelize(Seq.fill(5)(Calendar.getInstance.getTimeInMillis).map { x => longToBytes(x) }))
      }
    }
  }
}

class MemSQLTransformer extends ByteArrayTransformer {
  override def transform(sqlContext: SQLContext, from: RDD[Array[Byte]], config: PhaseConfig): DataFrame = {
    val dateFormat = new SimpleDateFormat()
    val transformed = from.map(bytesToLong).map { x =>
      Row(dateFormat.format(x))
    }
    sqlContext.createDataFrame(transformed, StructType(Array(StructField("val_datetime", TimestampType, false))))
  }
}

class KafkaValueTransformer extends ByteArrayTransformer {
  override def transform(sqlContext: SQLContext, from: RDD[Array[Byte]], config: PhaseConfig): DataFrame = {
    val transformed = from.map { x => Row(bytesToUTF8String(x)) }
    sqlContext.createDataFrame(transformed, StructType(Array(StructField("val_string", StringType, false))))
  }
}
