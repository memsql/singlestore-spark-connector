package com.memsql.spark.etl.api

import org.apache.spark.sql.DataFrame
import com.memsql.spark.connector._
import com.memsql.spark.etl.utils.PhaseLogger
import com.memsql.spark.etl.api.configs.{PhaseConfig, MemSQLLoadConfig, MemSQLKeyConfig, MemSQLExtraColumnConfig}

class MemSQLLoader extends Loader {
  val DefaultUpsertBatchSize = 1000
  var hasInserted: Boolean = false

  override def load(df: DataFrame, loadConfig: PhaseConfig, logger: PhaseLogger): Long = {
    val memSQLLoadConfig = loadConfig.asInstanceOf[MemSQLLoadConfig]
    val options = memSQLLoadConfig.options.getOrElse(memSQLLoadConfig.getDefaultOptions)

    if (!hasInserted) {
      df.createMemSQLTableFromSchema(memSQLLoadConfig.db_name,
                                     memSQLLoadConfig.table_name,
                                     keys = options.table_keys.getOrElse(List[MemSQLKeyConfig]()).map((k: MemSQLKeyConfig) => k.toMemSQLKey),
                                     extraCols = options.table_extra_columns.getOrElse(List[MemSQLExtraColumnConfig]()).map((k: MemSQLExtraColumnConfig) => k.toMemSQLExtraColumn),
                                     ifNotExists = true)
      hasInserted = true
    }

    if (memSQLLoadConfig.dry_run) {
      df.rdd.map(x => 0).reduce(_+_)
    } else {
      df.saveToMemSQL(memSQLLoadConfig.db_name,
        memSQLLoadConfig.table_name,
        onDuplicateKeySql = options.on_duplicate_key_sql.getOrElse(""),
        upsertBatchSize = options.upsert_batch_size.getOrElse(DefaultUpsertBatchSize),
        useKeylessShardedOptimization = options.use_keyless_sharding_optimization.getOrElse(false))
    }
  }
}
