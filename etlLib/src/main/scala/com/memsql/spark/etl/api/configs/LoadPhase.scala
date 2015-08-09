package com.memsql.spark.etl.api.configs

import spray.json._
import LoadPhaseKind._

case class MemSQLLoadConfig(db_name: String, table_name: String) extends PhaseConfig

case class UserLoadConfig(class_name: String, value: String) extends PhaseConfig

object LoadPhase extends DefaultJsonProtocol {
  val userConfigFormat = jsonFormat2(UserLoadConfig)
  val memSQLConfigFormat = jsonFormat2(MemSQLLoadConfig)

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
