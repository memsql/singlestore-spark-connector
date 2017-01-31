package com.memsql.spark.connector

import com.memsql.spark.connector.sql.{ColumnDefinition, MemSQLKey}
import org.apache.spark.sql.SaveMode


object CreateMode extends Enumeration {
  type CreateMode = Value
  val DatabaseAndTable, Table, Skip = Value
}
import CreateMode.CreateMode

object CompressionType extends Enumeration {
  type CompressionType = Value
  val GZip, Skip = Value
}
import CompressionType.CompressionType

case class SaveToMemSQLConf(saveMode: SaveMode,
                            createMode: CreateMode,
                            onDuplicateKeySQL: Option[String],
                            insertBatchSize: Int,
                            loadDataCompression: CompressionType,
                            useKeylessShardingOptimization: Boolean,
                            extraColumns: Seq[ColumnDefinition],
                            extraKeys: Seq[MemSQLKey],
                            dryRun: Boolean)

object SaveToMemSQLConf {
  /**
    * Try to create a [[SaveToMemSQLConf]] from the provided
    * parameters map, falling back to defaults in the
    * MemSQLConf as needed.
    */
  def apply(memsqlConf: MemSQLConf,
            mode: Option[SaveMode] = None,
            params: Map[String, String] = Map.empty): SaveToMemSQLConf = {

    SaveToMemSQLConf(
      saveMode = mode.getOrElse(memsqlConf.defaultSaveMode),
      createMode = CreateMode.withName(params.getOrElse("createMode", memsqlConf.defaultCreateMode.toString)),
      onDuplicateKeySQL = params.get("onDuplicateKeySQL"),
      insertBatchSize = params.getOrElse("insertBatchSize", memsqlConf.defaultInsertBatchSize.toString).toInt,
      loadDataCompression = CompressionType.withName(params.getOrElse(
        "loadDataCompression", memsqlConf.defaultLoadDataCompression.toString)),
      useKeylessShardingOptimization = params.getOrElse("useKeylessShardingOptimization", "false").toBoolean,
      extraColumns = Nil,
      extraKeys = Nil,
      dryRun = params.getOrElse("dryRun", "false").toBoolean
    )
  }
}
