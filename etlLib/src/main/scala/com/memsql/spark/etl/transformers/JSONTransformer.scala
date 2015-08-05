package com.memsql.spark.etl.transformers

import org.apache.spark.sql.DataFrame
import com.memsql.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

import com.memsql.spark.etl.api.configs.PhaseConfig
import com.memsql.spark.etl.utils.JSONPath
import com.memsql.spark.etl.utils.JSONUtils
import com.memsql.spark.etl.api.Transformer

object JSONTransformer {
  def makeFlattenedTransformer[S](
    flattenedPaths: Array[JSONPath],
    preprocess: S => String = ((k : S) => k.toString)
  )
  : Transformer[S] = {
    new Transformer[S] {
      override def transform(sqlContext: SQLContext, rdd: RDD[S], transformConfig: PhaseConfig): DataFrame = {
        JSONUtils.JSONRDDToDataFrame(flattenedPaths, sqlContext, rdd.map(preprocess))
      }
    }
  }
}
