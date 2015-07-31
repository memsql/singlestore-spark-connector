package com.memsql.spark.etl.api

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SQLContext

trait ETLPipeline[S] extends Extractor[S] with Transformer[S] with Loader {
  def run(ssc:StreamingContext, sqlContext: SQLContext): Unit = {
    val inputDStream = extract(ssc)
    inputDStream.foreachRDD { rdd => 
      val df = transform(sqlContext, rdd)
      load(df)
                
      Console.println(s"${inputDStream.count()} rows after extract")
      Console.println(s"${df.count()} rows after transform")

    }
  }
}
