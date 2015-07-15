package com.memsql.etl.api

import org.apache.spark.streaming.dstream.DStream

trait Transformer[From, To] extends Serializable {
  def transform(from: DStream[From]): DStream[To]
}
