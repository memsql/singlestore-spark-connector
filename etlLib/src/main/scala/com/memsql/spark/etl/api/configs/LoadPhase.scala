package com.memsql.spark.etl.api.configs

import spray.json._
import LoadPhaseKind._
import com.memsql.spark.connector.dataframe._

object MemSQLKeyType extends Enumeration {
  type MemSQLKeyType = Value
  val Shard, Key, PrimaryKey, UniqueKey, KeyUsingClusteredColumnStore = Value
}
import MemSQLKeyType._

object MemSQLTableConfig extends Enumeration {
  type MemSQLTableConfig = Value
  val RowStore, ColumnStore, Custom = Value
}
import MemSQLTableConfig.MemSQLTableConfig

case class MemSQLKeyConfig(key_type: MemSQLKeyType, column_names: List[String]) {
  def toMemSQLKey : MemSQLKey = {
    key_type match {
      case MemSQLKeyType.Shard => com.memsql.spark.connector.dataframe.Shard(column_names.toArray)
      case MemSQLKeyType.Key => com.memsql.spark.connector.dataframe.Key(column_names.toArray)
      case MemSQLKeyType.PrimaryKey => com.memsql.spark.connector.dataframe.PrimaryKey(column_names.toArray)
      case MemSQLKeyType.KeyUsingClusteredColumnStore => com.memsql.spark.connector.dataframe.KeyUsingClusteredColumnStore(column_names.toArray)
      case MemSQLKeyType.UniqueKey => com.memsql.spark.connector.dataframe.UniqueKey(column_names.toArray)
    }
  }
}

case class MemSQLExtraColumnConfig(name: String,
                                   col_type: String,
                                   nullable: Option[Boolean],
                                   default_value: Option[String],
                                   persisted: Option[String]) {
  def toMemSQLExtraColumn: MemSQLExtraColumn = {
    MemSQLExtraColumn(name, col_type, nullable.getOrElse(true), default_value.orNull, persisted.orNull)
  }
}

case class LoadConfigOptions(
  on_duplicate_key_sql: Option[String]=None,
  upsert_batch_size: Option[Int]=None,
  table_keys: Option[List[MemSQLKeyConfig]]=None,
  table_extra_columns: Option[List[MemSQLExtraColumnConfig]]=None,
  use_keyless_sharding_optimization: Option[Boolean]=None)

case class MemSQLLoadConfig(
  db_name: String,
  table_name: String,
  table_config: Option[MemSQLTableConfig],
  options: Option[LoadConfigOptions]
) extends PhaseConfig {
  def getDefaultOptions: LoadConfigOptions = {
    val keyType = table_config match {
      case Some(MemSQLTableConfig.ColumnStore) => MemSQLKeyType.KeyUsingClusteredColumnStore
      case _ => MemSQLKeyType.Key
    }

    LoadConfigOptions(
      table_extra_columns=Some(List(MemSQLExtraColumnConfig(
        name="memsql_insert_time",
        col_type="TIMESTAMP",
        nullable=Some(false),
        default_value=Some("CURRENT_TIMESTAMP"),
        persisted=None
      ))),
      table_keys=Some(List(
        MemSQLKeyConfig(key_type=MemSQLKeyType.Shard, column_names=List()),
        MemSQLKeyConfig(key_type=keyType, column_names=List("memsql_insert_time"))
      ))
    )
  }
}

case class UserLoadConfig(class_name: String, value: String) extends PhaseConfig

object LoadPhaseImplicits extends JsonEnumProtocol {
  implicit val memSQLKeyTypeTypeFormat = jsonEnum(MemSQLKeyType)
  implicit val memSQLkeyConfigFormat = jsonFormat2(MemSQLKeyConfig)
  implicit val memSQLextraColumnConfigFormat = jsonFormat5(MemSQLExtraColumnConfig)
  implicit val memSQLTableTypeTypeFormat = jsonEnum(MemSQLTableConfig)
  implicit val memSQLOptionsFormat = jsonFormat5(LoadConfigOptions)
}
import LoadPhaseImplicits._

object LoadPhase extends DefaultJsonProtocol {
  val userConfigFormat = jsonFormat2(UserLoadConfig)
  val memSQLConfigFormat = jsonFormat4(MemSQLLoadConfig)

  def readConfig(kind: LoadPhaseKind, config: JsValue): PhaseConfig = {
    kind match {
      case LoadPhaseKind.User => userConfigFormat.read(config)
      case LoadPhaseKind.MemSQL => memSQLConfigFormat.read(config)
    }
  }

  def writeConfig(kind: LoadPhaseKind, config: PhaseConfig): JsValue = {
    kind match {
      case LoadPhaseKind.User => userConfigFormat.write(config.asInstanceOf[UserLoadConfig])
      case LoadPhaseKind.MemSQL => memSQLConfigFormat.write(config.asInstanceOf[MemSQLLoadConfig])
    }
  }
}
