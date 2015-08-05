package com.memsql.spark.etl.api

import org.apache.spark.sql.DataFrame
import com.memsql.spark.connector._
import com.memsql.spark.etl.api.configs.PhaseConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

// A Transformer which generates single column JSON DataFrames
// Appropriate for working with Loaders which load into tables with a single (Parquette) JSON column and possibly a key column or columns with default/computed/timestamp columns
//
object JSONTransformer extends Serializable {

  def makeSimpleJSONExtractor[S](
    jsonColumnName: String,
    preprocess: S => String
  )
  : Transformer[S] = {
    new Transformer[S] {
      override def transform(sqlContext: SQLContext, rdd: RDD[S], transformConfig: PhaseConfig): DataFrame = {
        val transformedRDD = rdd.map(r => Row(preprocess(r)))
        val schema = StructType(Array(StructField(jsonColumnName, StringType, true)))
        sqlContext.createDataFrame(transformedRDD, schema)
      }
    }
  }

  def makeSimpleJSONKeyValueExtractor[K,V](
    jsonColumnName: String,
    keyColumnName: String = null,
    keyColumnType: DataType = StringType,
    preprocessKey: K => Any = ((k : K) => k.toString),
    preprocessValue : V => String = ((v : V) => v.toString)
  )
  : Transformer[(K,V)] = {
    new Transformer[(K,V)] {
      override def transform(sqlContext: SQLContext, rdd: RDD[(K,V)], transformConfig: PhaseConfig): DataFrame = {
        val transformedRDD = if (keyColumnName == null) {
          rdd.map(r => Row(preprocessValue(r._2)))
        } else {
          rdd.map(r => Row(preprocessKey(r._1), preprocessValue(r._2)))
        }
        val schema = if (keyColumnName == null) {
          StructType(Array(StructField(jsonColumnName, StringType, true)))
        } else {
          StructType(Array(StructField(keyColumnName, keyColumnType, false), StructField(jsonColumnName, StringType, true)))
        }
        sqlContext.createDataFrame(transformedRDD, schema)
      }
    }
  }
}
