package com.memsql.spark.etl.api.configs

import spray.json._
import LoadPhaseKind._
import com.memsql.spark.connector.dataframe._

object MemSQLKeyType extends Enumeration {
  type MemSQLKeyType = Value
  val Shard, Key, PrimaryKey, UniqueKey, KeyUsingClusteredColumnStore = Value
}
import MemSQLKeyType._

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

case class MemSQLLoadConfig(db_name: String, 
                            table_name: String,
                            on_duplicate_key_sql: Option[String],
                            upsert_batch_size: Option[Int],
                            table_keys: Option[List[MemSQLKeyConfig]],
                            table_extra_columns: Option[List[MemSQLExtraColumnConfig]],
                            use_keyless_sharding_optimization: Option[Boolean]
                          ) extends PhaseConfig 

case class UserLoadConfig(class_name: String, value: String) extends PhaseConfig

object KeyTypeJsonProtocol extends JsonEnumProtocol {
  implicit val memSQLKeyTypeTypeFormat = jsonEnum(MemSQLKeyType)
  implicit val memSQLkeyConfigFormat = jsonFormat2(MemSQLKeyConfig)
  implicit val memSQLextraColumnConfigFormat = jsonFormat5(MemSQLExtraColumnConfig)
}
import KeyTypeJsonProtocol._

object LoadPhase extends DefaultJsonProtocol {
  val userConfigFormat = jsonFormat2(UserLoadConfig)
  val memSQLConfigFormat = jsonFormat7(MemSQLLoadConfig)
  

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
