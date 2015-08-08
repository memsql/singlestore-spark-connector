package com.memsql.spark.etl.api

import org.apache.spark.sql.DataFrame
import com.memsql.spark.connector._
import com.memsql.spark.etl.api.configs.{PhaseConfig, MemSQLLoadConfig, MemSQLKeyConfig}

class MemSQLLoader extends Loader {
  val DefaultUpsertBatchSize = 1000  
  var hasInserted: Boolean = false
  override def load(df: DataFrame, loadConfig: PhaseConfig): Unit = {
    val memSQLLoadConfig = loadConfig.asInstanceOf[MemSQLLoadConfig]
    if (!hasInserted) {
      hasInserted = true      
      df.createMemSQLTableFromSchema(memSQLLoadConfig.db_name, 
                                     memSQLLoadConfig.table_name,
                                     keys = memSQLLoadConfig.table_keys.getOrElse(List[MemSQLKeyConfig]()).map((k: MemSQLKeyConfig) => k.toMemSQLKey).toArray,
                                     ifNotExists = true)
    }
    df.saveToMemSQL(memSQLLoadConfig.db_name, 
                    memSQLLoadConfig.table_name,
                    onDuplicateKeySql = memSQLLoadConfig.on_duplicate_key_sql.getOrElse(""),
                    upsertBatchSize = memSQLLoadConfig.upsert_batch_size.getOrElse(DefaultUpsertBatchSize))
  }
}
