package com.memsql.spark.connector

import java.net.InetAddress
import com.memsql.spark.connector.util.MemSQLConnectionInfo
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import com.memsql.spark.connector.CompressionType.CompressionType
import com.memsql.spark.connector.CreateMode.CreateMode


/**
 * Configuration for a MemSQL cluster. By default these parameters are set by the corresponding
 * value in the Spark configuration.
 *
 * @param masterHost Hostname of the MemSQL Master Aggregator. Corresponds to "spark.memsql.host"
 *                   in the Spark configuration.
 * @param masterPort Port of the MemSQL Master Aggregator. Corresponds to "spark.memsql.port"
 *                   in the Spark configuration.
 * @param user Username to use when connecting to the MemSQL Master Aggregator. Corresponds to
 *             "spark.memsql.user" in the Spark configuration.
 * @param password Password to use when connecting to the MemSQL Master Aggregator. Corresponds to
 *                 "sparkk.memsql.password" in the Spark configuration.
 * @param defaultDBName The default database to use when connecting to the cluster. Corresponds to
 *                      "spark.memsql.defaultDatabase" in the Spark configuration.
 * @param defaultSaveMode The default [[org.apache.spark.sql.SaveMode]] to use when writingsaving
 *                        [[org.apache.spark.sql.DataFrame]]s to a MemSQL table. Corresponds to
 *                        "spark.memsql.defaultSaveMode" in the Spark configuration.
 * @param defaultCreateMode The default [[com.memsql.spark.connector.CreateMode]] to use
 *                          when creating a MemSQL table. Corresponds to "spark.memsql.defaultCreateMode"
 *                          in the Spark configuration.
 * @param defaultInsertBatchSize The default batch insert size to use when writing to a
 *                               MemSQL table using [[com.memsql.spark.connector.InsertStrategy]].
 *                               Corresponds to "spark.memsql.defaultInsertBatchSize" in the Spark
 *                               configuration.
 * @param defaultLoadDataCompression The default [[com.memsql.spark.connector.CompressionType]] to
 *                                   use when writing to a MemSQL table using
 *                                   [[com.memsql.spark.connector.LoadDataStrategy]]. Corresponds to
 *                                   "spark.memsql.defaultLoadDataCompression" in the Spark configuration.
 */
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

  def getDefaultHost: String = InetAddress.getLocalHost.getHostAddress

  def apply(sparkConf: SparkConf): MemSQLConf =
    MemSQLConf(
      masterHost = sparkConf.get("spark.memsql.host", getDefaultHost),
      masterPort = sparkConf.getInt("spark.memsql.port", DEFAULT_PORT),
      user = sparkConf.get("spark.memsql.user", DEFAULT_USER),
      password = sparkConf.get("spark.memsql.password", DEFAULT_PASS),
      defaultDBName = sparkConf.get("spark.memsql.defaultDatabase", DEFAULT_DATABASE),
      defaultSaveMode = SaveMode.valueOf(sparkConf.get("spark.memsql.defaultSaveMode", DEFAULT_SAVE_MODE.name)),
      defaultCreateMode = CreateMode.withName(
        sparkConf.get("spark.memsql.defaultCreateMode", DEFAULT_CREATE_MODE.toString)),
      defaultInsertBatchSize = sparkConf.getInt("spark.memsql.defaultInsertBatchSize", DEFAULT_INSERT_BATCH_SIZE),
      defaultLoadDataCompression = CompressionType.withName(
        sparkConf.get("spark.memsql.defaultLoadDataCompression", DEFAULT_LOAD_DATA_COMPRESSION.toString))
    )
}
