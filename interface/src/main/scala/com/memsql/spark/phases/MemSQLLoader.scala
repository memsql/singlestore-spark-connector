package com.memsql.spark.phases

import com.memsql.spark.connector._
import com.memsql.spark.etl.api.configs.MemSQLLoadConfig
import com.memsql.spark.etl.api.{Loader, PhaseConfig}
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.memsql.{SaveToMemSQLConf, CreateMode}

class MemSQLLoader extends Loader {
  var hasInserted: Boolean = false

  override def load(df: DataFrame, loadConfig: PhaseConfig, logger: PhaseLogger): Long = {
    val memSQLConf = df.getMemSQLConf
    val memSQLLoadConfig = loadConfig.asInstanceOf[MemSQLLoadConfig]
    val options = memSQLLoadConfig.options.getOrElse(memSQLLoadConfig.getDefaultOptions)

    var createMode = options.createMode.getOrElse(CreateMode.DatabaseAndTable)
    if (hasInserted) {
      createMode = CreateMode.Skip
    }

    val saveConfig = SaveToMemSQLConf(
      saveMode = options.getSaveMode,
      createMode = createMode,
      onDuplicateKeySQL = options.on_duplicate_key_sql,
      insertBatchSize = options.upsert_batch_size.getOrElse(memSQLConf.defaultInsertBatchSize),
      loadDataCompression = memSQLConf.defaultLoadDataCompression,
      useKeylessShardingOptimization = options.use_keyless_sharding_optimization.getOrElse(false),
      extraColumns = options.getExtraColumns,
      extraKeys = options.getExtraKeys,
      dryRun = memSQLLoadConfig.dry_run
    )

    val numInserted = df.saveToMemSQL(memSQLLoadConfig.getTableIdentifier, saveConfig)
    hasInserted = true
    numInserted
  }
}
