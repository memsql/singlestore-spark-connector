package com.memsql.spark.etl.api

import org.apache.spark.sql.DataFrame
import com.memsql.spark.connector._

object MemSQLLoader extends Serializable {

  def makeMemSQLLoader(
    dbName: String,
    tableName: String,
    dbHost: String = null,
    dbPort: Int = -1,
    user: String = null,
    password: String = null,
    onDuplicateKeySql: String = "",
    useInsertIgnore: Boolean = false,
    upsertBatchSize: Int = 10000)
  : Loader = {
    new Loader {
      override def load(df: DataFrame): Unit = {
        df.saveToMemSQL(
          dbName,
          tableName,
          dbHost,
          dbPort,
          user,
          password,
          onDuplicateKeySql,
          useInsertIgnore,
          upsertBatchSize)
      }
    }
  }
}

