package com.memsql.spark.etl.api

import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.api.java.JavaDStream
import com.memsql.spark.connector._

object SimpleLoader extends Serializable {

  def stringRowLoad(
                   dbName: String,
                   tableName: String,
                   dbHost: String = null,
                   dbPort: Int = -1,
                   user: String = null,
                   password: String = null,
                   onDuplicateKeySql: String = "",
                   useInsertIgnore: Boolean = false,
                   upsertBatchSize: Int = 10000)
  : Loader[Row] = {
    new Loader[Row] {
      override def load(stream: DStream[Row]): Unit = {
        stream.foreachRDD(rdd => rdd.saveToMemSQL(
          dbName,
          tableName,
          dbHost,
          dbPort,
          user,
          password,
          onDuplicateKeySql,
          useInsertIgnore,
          upsertBatchSize)
        )
      }
    }
  }

  def stringRowJavaLoad(
                         dbName: String,
                         tableName: String,
                         dbHost: String = null,
                         dbPort: Int = -1,
                         user: String = null,
                         password: String = null,
                         onDuplicateKeySql: String = "",
                         useInsertIgnore: Boolean = false,
                         upsertBatchSize: Int = 10000)
  : JavaLoader[Row] = {
    new JavaLoader[Row] {
      override def load(stream: JavaDStream[Row]): Unit = {
        stream.dstream.foreachRDD(rdd => rdd.saveToMemSQL(
          dbName,
          tableName,
          dbHost,
          dbPort,
          user,
          password,
          onDuplicateKeySql,
          useInsertIgnore,
          upsertBatchSize)
        )
      }
    }
  }
}
