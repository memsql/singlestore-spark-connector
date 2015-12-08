package com.memsql.spark.etl.api.configs

import com.memsql.spark.connector.sql
import com.memsql.spark.connector.sql._
import com.memsql.spark.etl.api.PhaseConfig
import com.memsql.spark.etl.api.configs.LoadPhaseKind._
import com.memsql.spark.etl.utils.JsonEnumProtocol
import org.apache.spark.sql.SaveMode
import spray.json._

object MemSQLKeyType extends Enumeration {
  type MemSQLKeyType = Value
  val Shard, Key, PrimaryKey, UniqueKey, KeyUsingClusteredColumnStore = Value
}
import com.memsql.spark.etl.api.configs.MemSQLKeyType._

object MemSQLTableConfig extends Enumeration {
  type MemSQLTableConfig = Value
  val RowStore, ColumnStore, Custom = Value
}
import com.memsql.spark.etl.api.configs.MemSQLTableConfig.MemSQLTableConfig

object MemSQLDupKeyBehavior extends Enumeration {
  type MemSQLDupKeyBehavior = Value
  val Replace, Ignore, Update = Value
}
import com.memsql.spark.etl.api.configs.MemSQLDupKeyBehavior._

case class MemSQLKeyConfig(key_type: MemSQLKeyType, column_names: List[String]) {
  def toMemSQLKey : MemSQLKey = {
    val columns = column_names.map(Column(_))
    key_type match {
      case MemSQLKeyType.Shard => sql.Shard(columns)
      case MemSQLKeyType.Key => sql.Key(columns)
      case MemSQLKeyType.PrimaryKey => sql.PrimaryKey(columns)
      case MemSQLKeyType.UniqueKey => sql.UniqueKey(columns)
      case MemSQLKeyType.KeyUsingClusteredColumnStore
        => sql.KeyUsingClusteredColumnStore(columns)
    }
  }
}

case class MemSQLColumnConfig(name: String,
                              col_type: String,
                              nullable: Option[Boolean],
                              default_sql: Option[String],
                              persisted: Option[String]) {
  def toMemSQLColumn: MemSQLColumn = {
    AdvancedColumn(name, col_type, nullable.getOrElse(true), defaultSQL = default_sql, persisted = persisted)
  }
}

case class LoadConfigOptions(on_duplicate_key_sql: Option[String]=None,
                             upsert_batch_size: Option[Int]=None,
                             table_keys: Option[List[MemSQLKeyConfig]]=None,
                             table_extra_columns: Option[List[MemSQLColumnConfig]]=None,
                             use_keyless_sharding_optimization: Option[Boolean]=None,
                             duplicate_key_behavior: Option[MemSQLDupKeyBehavior]=None) {

  if (on_duplicate_key_sql.isDefined && duplicate_key_behavior != Some(MemSQLDupKeyBehavior.Update)) {
    throw new IllegalArgumentException("If on_duplicate_key_sql is defined, duplicate_key_behavior must be Update.")
  }
  if (on_duplicate_key_sql.isEmpty && duplicate_key_behavior == Some(MemSQLDupKeyBehavior.Update)) {
    throw new IllegalArgumentException("If duplicate_key_behavior is set to Update, then on_duplicate_key_sql must be specified.")
  }

  def getSaveMode: SaveMode = {
    duplicate_key_behavior match {
      case Some(MemSQLDupKeyBehavior.Replace) => SaveMode.Overwrite
      case Some(MemSQLDupKeyBehavior.Ignore) => SaveMode.Ignore
      case Some(MemSQLDupKeyBehavior.Update) => SaveMode.Overwrite
      case _ => SaveMode.ErrorIfExists
    }
  }

  def getExtraColumns: Seq[MemSQLColumn] = table_extra_columns match {
    case Some(cols) => cols.map(_.toMemSQLColumn)
    case None => Nil
  }

  def getExtraKeys: Seq[MemSQLKey] = table_keys match {
    case Some(cols) => cols.map(_.toMemSQLKey)
    case None => Nil
  }
}

case class MemSQLLoadConfig(
  db_name: String,
  table_name: String,
  table_config: Option[MemSQLTableConfig],
  options: Option[LoadConfigOptions],
  dry_run: Boolean = false
) extends PhaseConfig {
  def getTableIdentifier: TableIdentifier = TableIdentifier(db_name, table_name)

  def getDefaultOptions: LoadConfigOptions = {
    val keyType = table_config match {
      case Some(MemSQLTableConfig.ColumnStore) => MemSQLKeyType.KeyUsingClusteredColumnStore
      case _ => MemSQLKeyType.Key
    }

    LoadConfigOptions(
      table_extra_columns=Some(List(MemSQLColumnConfig(
        name="memsql_insert_time",
        col_type="TIMESTAMP",
        nullable=Some(false),
        default_sql=Some("CURRENT_TIMESTAMP"),
        persisted=None
      ))),
      table_keys=Some(List(
        MemSQLKeyConfig(key_type=MemSQLKeyType.Shard, column_names=List()),
        MemSQLKeyConfig(key_type=keyType, column_names=List("memsql_insert_time"))
      )),
      use_keyless_sharding_optimization = Some(false)
    )
  }
}

object LoadPhaseImplicits extends JsonEnumProtocol {
  implicit val memSQLKeyTypeTypeFormat = jsonEnum(MemSQLKeyType)
  implicit val memSQLkeyConfigFormat = jsonFormat2(MemSQLKeyConfig)
  implicit val memSQLextraColumnConfigFormat = jsonFormat5(MemSQLColumnConfig)
  implicit val memSQLTableTypeTypeFormat = jsonEnum(MemSQLTableConfig)
  implicit val memSQLErrorBehaviorFormat = jsonEnum(MemSQLDupKeyBehavior)
  implicit val memSQLOptionsFormat = jsonFormat6(LoadConfigOptions)
}
import com.memsql.spark.etl.api.configs.LoadPhaseImplicits._

object LoadPhase extends DefaultJsonProtocol {
  val memSQLConfigFormat = jsonFormat5(MemSQLLoadConfig)

  def readConfig(kind: LoadPhaseKind, config: JsValue): PhaseConfig = {
    kind match {
      case LoadPhaseKind.MemSQL => memSQLConfigFormat.read(config)
    }
  }

  def writeConfig(kind: LoadPhaseKind, config: PhaseConfig): JsValue = {
    kind match {
      case LoadPhaseKind.MemSQL => memSQLConfigFormat.write(config.asInstanceOf[MemSQLLoadConfig])
    }
  }
}
