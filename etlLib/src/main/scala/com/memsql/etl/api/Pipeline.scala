package com.memsql.etl.api

import org.apache.spark.streaming.StreamingContext

trait Pipeline[S, R] extends Extractor[S] with Transformer[S, R] with Loader[R] {
  def run(sc:StreamingContext): Unit = {
    val inputDStream = this.extract(sc)
    val transformedDStream = this.transform(inputDStream)
    this.load(transformedDStream)

    Console.println(s"${inputDStream.count()} rows after extract")
    Console.println(s"${transformedDStream.count()} rows after transform")

    sc.start()
    sc.awaitTermination()
  }
}
