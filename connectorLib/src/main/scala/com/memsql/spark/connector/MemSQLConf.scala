package com.memsql.spark.connector

import java.net.InetAddress
import com.memsql.spark.connector.util.MemSQLConnectionInfo
import org.apache.spark.SparkConf

case class MemSQLConf(masterHost: String,
                      masterPort: Int,
                      user: String,
                      password: String,
                      pushdownEnabled: Boolean,
                      defaultDbName: String = "") {

  val masterConnectionInfo: MemSQLConnectionInfo =
    MemSQLConnectionInfo(masterHost, masterPort, user, password, defaultDbName)
}

object MemSQLConf {
  val DEFAULT_PORT = 3306
  val DEFAULT_USER = "root"
  val DEFAULT_PASS = ""
  val DEFAULT_PUSHDOWN_ENABLED = true
  val DEFAULT_DATABASE = ""

  def apply(sparkConf: SparkConf): MemSQLConf =
    MemSQLConf(
      masterHost = sparkConf.get("memsql.host", InetAddress.getLocalHost.getHostAddress),
      masterPort = sparkConf.getInt("memsql.port", DEFAULT_PORT),
      user = sparkConf.get("memsql.user", DEFAULT_USER),
      password = sparkConf.get("memsql.password", DEFAULT_PASS),
      pushdownEnabled = sparkConf.getBoolean("memsql.pushdown", DEFAULT_PUSHDOWN_ENABLED),
      defaultDbName = sparkConf.get("memsql.default_database", DEFAULT_DATABASE)
    )
}
