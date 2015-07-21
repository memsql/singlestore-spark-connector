package com.memsql.spark.etl.api

import org.apache.spark.streaming.StreamingContext

trait ETLPipeline[S, R] extends Extractor[S] with Transformer[S, R] with Loader[R] {
  def run(sc:StreamingContext): Unit = {
    val inputDStream = extract(sc)
    val transformedDStream = transform(inputDStream)
    load(transformedDStream)

    Console.println(s"${inputDStream.count()} rows after extract")
    Console.println(s"${transformedDStream.count()} rows after transform")

    sc.start()
    sc.awaitTermination()
  }
}
