package com.memsql.spark.etl.api

import org.apache.spark.streaming.{Time, StreamingContext}
import org.apache.spark.sql.SQLContext

object ETLPipeline {
  val BATCH_DURATION = 5000
}

trait ETLPipeline[S]  {
  
  val extractor: Extractor[S] 
  val transformer: Transformer[S]
  val loader: Loader

  def run(ssc:StreamingContext, sqlContext: SQLContext): Unit = {
    val inputDStream = extractor.extract(ssc)
    var time: Long = 0

    // manually compute the next RDD in the DStream so that we can sidestep issues with
    // adding inputs to the streaming context at runtime
    while (true) {
      time = System.currentTimeMillis

      inputDStream.compute(Time(time)) match {
        case Some(rdd) => {
          val df = transformer.transform(sqlContext, rdd)
          loader.load(df)

          Console.println(s"${inputDStream.count()} rows after extract")
          Console.println(s"${df.count()} rows after transform")
        }
        case None =>
      }

      Thread.sleep(Math.max(ETLPipeline.BATCH_DURATION - (System.currentTimeMillis - time), 0))
    }
  }
}
