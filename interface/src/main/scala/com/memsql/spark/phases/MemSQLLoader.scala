package com.memsql.spark.phases

import com.memsql.spark.connector._
import com.memsql.spark.connector.OnDupKeyBehavior._
import com.memsql.spark.etl.api.configs.{MemSQLExtraColumnConfig, MemSQLKeyConfig, MemSQLDupKeyBehavior, MemSQLLoadConfig}
import com.memsql.spark.etl.api.{Loader, PhaseConfig}
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.sql.DataFrame

class MemSQLLoader extends Loader {
  val DefaultUpsertBatchSize = 1000
  var hasInserted: Boolean = false

  override def load(df: DataFrame, loadConfig: PhaseConfig, logger: PhaseLogger): Long = {
    val memSQLLoadConfig = loadConfig.asInstanceOf[MemSQLLoadConfig]
    val options = memSQLLoadConfig.options.getOrElse(memSQLLoadConfig.getDefaultOptions)

    if (!hasInserted) {
      val extraColumns = options.table_extra_columns.getOrElse(List[MemSQLExtraColumnConfig]())
                         .map((k: MemSQLExtraColumnConfig) => k.toMemSQLExtraColumn)
      df.createMemSQLTableFromSchema(memSQLLoadConfig.db_name,
                                     memSQLLoadConfig.table_name,
                                     keys = options.table_keys.getOrElse(List[MemSQLKeyConfig]()).map((k: MemSQLKeyConfig) => k.toMemSQLKey),
                                     extraCols = extraColumns,
                                     ifNotExists = true)
      hasInserted = true
    }

    val onDuplicateKeyBehavior = options.duplicate_key_behavior match {
      case Some(MemSQLDupKeyBehavior.Replace) => Some(OnDupKeyBehavior.Replace)
      case Some(MemSQLDupKeyBehavior.Ignore) => Some(OnDupKeyBehavior.Ignore)
      case Some(MemSQLDupKeyBehavior.Update) => Some(OnDupKeyBehavior.Update)
      case _ => None
    }

    if (memSQLLoadConfig.dry_run) {
      df.rdd.map(x => 0).reduce(_ + _)
    } else {
      df.saveToMemSQL(memSQLLoadConfig.db_name,
        memSQLLoadConfig.table_name,
        onDuplicateKeyBehavior = onDuplicateKeyBehavior,
        onDuplicateKeySql = options.on_duplicate_key_sql.getOrElse(""),
        upsertBatchSize = options.upsert_batch_size.getOrElse(DefaultUpsertBatchSize),
        useKeylessShardedOptimization = options.use_keyless_sharding_optimization.getOrElse(false))
    }
  }
}
