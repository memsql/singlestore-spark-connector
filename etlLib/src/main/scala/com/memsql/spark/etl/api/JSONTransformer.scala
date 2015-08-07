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

  /**
   * Produces dataframes with a single StringType column containing the json data.
   * Function `preprocess` is called on each row
   *
   * NOTE: The resulting dataframe is suitable for loading to a target table that has additional columns with defaults (including `TIMESTAMP default CURRENT_TIME` and computed columns).
   *
   * @param jsonColumnName the name of the StringType column.
   * @param preprocess a function to turn the input RDD into a JSON string
   */
  def makeSimpleJSONTransformer[S](
    jsonColumnName: String,
    preprocess: S => String = ((s : S) => s.toString)
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

  /**
   * Produces dataframes with a key column and a StringType column containing the json data from an RDD[(K,V)].
   * This is suitable, for instance, for processing a KafkaStream of JSON blobs.
   *
   * NOTE: The resulting dataframe is suitable for loading to a target table that has additional columns with defaults (including `TIMESTAMP default CURRENT_TIME` and computed columns).
   * 
   * @param jsonColumnName the name of the StringType column.
   * @param keyColumnName the name of the key column.  If null or ommited, the key is stripped.  
   * @param keyColumnType the type of the key column.
   * @param preprocessValue a function to turn the input RDD's values into a JSON string
   * @param preprocessKey a function to turn the input RDD's key into a Key
   */
  def makeSimpleJSONKeyValueTransformer[K,V](
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
