package com.memsql.etl.api

import org.apache.spark.streaming.api.java.JavaDStream


trait JavaTransformer[From, To] extends Serializable {

  def transform(from: JavaDStream[From]): JavaDStream[To]
}
