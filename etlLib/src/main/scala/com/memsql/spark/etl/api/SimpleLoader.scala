package com.memsql.spark.etl.api

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.api.java.JavaDStream
import com.memsql.spark.connector._

object SimpleLoader extends Serializable {

  def stringArrayLoad(
                       dbHost: String,
                       dbPort: Int,
                       user: String,
                       password: String,
                       dbName: String,
                       tableName: String,
                       onDuplicateKeySql: String = "",
                       insertBatchSize: Int = 10000)
  : Loader[Array[String]] = {
    new Loader[Array[String]] {
      override def load(stream: DStream[Array[String]]): Unit = {
        stream.foreachRDD(rdd => rdd.saveToMemsql(
          dbHost,
          dbPort,
          user,
          password,
          dbName,
          tableName,
          onDuplicateKeySql,
          insertBatchSize)
        )
      }
    }
  }

  def stringArrayJavaLoad(
                           dbHost: String,
                           dbPort: Int,
                           user: String,
                           password: String,
                           dbName: String,
                           tableName: String,
                           onDuplicateKeySql: String = "",
                           insertBatchSize: Int = 10000)
  : JavaLoader[Array[String]] = {
    new JavaLoader[Array[String]] {
      override def load(stream: JavaDStream[Array[String]]): Unit = {
        stream.dstream.foreachRDD(rdd => rdd.saveToMemsql(
          dbHost,
          dbPort,
          user,
          password,
          dbName,
          tableName,
          onDuplicateKeySql,
          insertBatchSize)
        )
      }
    }
  }
}
