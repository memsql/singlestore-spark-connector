package com.memsql.etl.api

import org.apache.spark.streaming.api.java.JavaDStream

trait JavaLoader[A] extends Serializable {
  def load(stream: JavaDStream[A])
}
