package com.memsql.etl.api

import org.apache.spark.streaming.dstream.DStream

trait Transformer[S, R] extends Serializable {
  def transform(from: DStream[S]): DStream[R]
}
