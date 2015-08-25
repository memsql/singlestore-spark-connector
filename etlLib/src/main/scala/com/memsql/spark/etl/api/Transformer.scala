package com.memsql.spark.etl.api

import java.nio.ByteBuffer

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import com.memsql.spark.etl.api.configs.PhaseConfig

trait Transformer[S] extends Serializable {
  def transform(sqlContext: SQLContext, rdd: RDD[S], transformConfig: PhaseConfig): DataFrame
}

trait ByteArrayTransformer extends Transformer[Array[Byte]] {
  def bytesToString(bytes: Array[Byte]): String = new String(bytes.map(_.toChar))
  def bytesToLong(bytes: Array[Byte]): Long = ByteBuffer.wrap(bytes).getLong
}
