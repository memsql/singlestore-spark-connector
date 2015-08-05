package com.memsql.spark.examples

import java.text.SimpleDateFormat
import java.util.Calendar

import com.memsql.spark.connector._
import com.memsql.spark.etl.api.ETLPipeline
import com.memsql.spark.etl.api.MemSQLLoader
import com.memsql.spark.etl.api.Extractor
import com.memsql.spark.etl.api.Transformer
import com.memsql.spark.etl.api.Loader
import com.memsql.spark.etl.api.configs.PhaseConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

case class MemSQLETLApp() extends ETLPipeline[Long] {
  override val extractor = new Extractor[Long] {
    override def extract(ssc: StreamingContext, config: PhaseConfig): InputDStream[Long] = {
      new InputDStream[Long](ssc) {
        override def stop(): Unit = {}

        override def start(): Unit = {}

        override def compute(validTime: Time): Option[RDD[Long]] = {
          Some(ssc.sparkContext.parallelize(Seq.fill(5)(Calendar.getInstance.getTimeInMillis)))
        }
      }
    }
  }

  override val transformer = new Transformer[Long] {
    override def transform(sqlContext: SQLContext, from: RDD[Long], config: PhaseConfig): DataFrame = {
      val dateFormat = new SimpleDateFormat()
      val transformed = from.map { x =>
        Row(dateFormat.format(x))
      }
      sqlContext.createDataFrame(transformed, StructType(Array(StructField("val_datetime",TimestampType,false))))
    }
  }

  override val loader = new MemSQLLoader
}
