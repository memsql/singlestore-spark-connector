package com.memsql.etl.api

import org.apache.spark.streaming.dstream.DStream

trait Loader[A] extends Serializable {
  def load(stream: DStream[A])
}
