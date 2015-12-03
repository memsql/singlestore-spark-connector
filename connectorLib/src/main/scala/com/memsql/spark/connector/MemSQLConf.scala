package com.memsql.spark.connector

import java.net.InetAddress
import com.memsql.spark.connector.util.MemSQLConnectionInfo
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.memsql.CompressionType.CompressionType
import org.apache.spark.sql.memsql.CreateMode.CreateMode
import org.apache.spark.sql.memsql.{CreateMode, CompressionType}

case class MemSQLConf(masterHost: String,
                      masterPort: Int,
                      user: String,
                      password: String,
                      defaultDBName: String,
                      defaultSaveMode: SaveMode,
                      defaultCreateMode: CreateMode,
                      defaultInsertBatchSize: Int,
                      defaultLoadDataCompression: CompressionType) {

  val masterConnectionInfo: MemSQLConnectionInfo =
    MemSQLConnectionInfo(masterHost, masterPort, user, password, defaultDBName)
}

object MemSQLConf {
  val DEFAULT_PORT = 3306
  val DEFAULT_USER = "root"
  val DEFAULT_PASS = ""
  val DEFAULT_PUSHDOWN_ENABLED = true
  val DEFAULT_DATABASE = ""

  val DEFAULT_SAVE_MODE = SaveMode.ErrorIfExists
  val DEFAULT_CREATE_MODE = CreateMode.DatabaseAndTable
  val DEFAULT_INSERT_BATCH_SIZE = 10000
  val DEFAULT_LOAD_DATA_COMPRESSION = CompressionType.GZip

  def apply(sparkConf: SparkConf): MemSQLConf =
    MemSQLConf(
      masterHost = sparkConf.get("memsql.host", InetAddress.getLocalHost.getHostAddress),
      masterPort = sparkConf.getInt("memsql.port", DEFAULT_PORT),
      user = sparkConf.get("memsql.user", DEFAULT_USER),
      password = sparkConf.get("memsql.password", DEFAULT_PASS),
      defaultDBName = sparkConf.get("memsql.defaultDatabase", DEFAULT_DATABASE),
      defaultSaveMode = SaveMode.valueOf(sparkConf.get("memsql.defaultSaveMode", DEFAULT_SAVE_MODE.name)),
      defaultCreateMode = CreateMode.withName(
        sparkConf.get("memsql.defaultCreateMode", DEFAULT_CREATE_MODE.toString)),
      defaultInsertBatchSize = sparkConf.getInt("memsql.defaultInsertBatchSize", DEFAULT_INSERT_BATCH_SIZE),
      defaultLoadDataCompression = CompressionType.withName(
        sparkConf.get("memsql.defaultLoadDataCompression", DEFAULT_LOAD_DATA_COMPRESSION.toString))
    )
}
