package com.memsql.spark.etl.api

import org.apache.spark.sql.DataFrame
import com.memsql.spark.connector._
import com.memsql.spark.etl.api.configs.{PhaseConfig, MemSQLLoadConfig}

class MemSQLLoader extends Loader {
  override def load(df: DataFrame, loadConfig: PhaseConfig): Unit = {
    val memSQLLoadConfig = loadConfig.asInstanceOf[MemSQLLoadConfig]
    df.saveToMemSQL(memSQLLoadConfig.db_name, memSQLLoadConfig.table_name)
  }
}
