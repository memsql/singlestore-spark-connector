package com.memsql.etl.api

import org.apache.spark.streaming.dstream.DStream

trait Loader[R] extends Serializable {
  def load(stream: DStream[R])
}
