package com.memsql.spark.phases

import com.memsql.spark.etl.api.{Extractor, PhaseConfig, UserExtractConfig}
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext


case class MySQLExtractTaskConfig()

case class MySQLExtractConfig(host: String,
                              port: Int,
                              user: String,
                              password: String,
                              db_name: String,
                              table_name: String,
                              task_config: MySQLExtractTaskConfig) extends PhaseConfig

class MySQLExtractor extends Extractor {
  override def next(ssc: StreamingContext,
                    time: Long,
                    sqlContext: SQLContext,
                    config: PhaseConfig,
                    batchInterval: Long,
                    logger: PhaseLogger): Option[DataFrame] = {
    val sc = ssc.sparkContext
    val mysqlConfig = config.asInstanceOf[UserExtractConfig]

    val host = mysqlConfig.getConfigString("host").get
    val port = mysqlConfig.getConfigInt("port").get
    val db_name = mysqlConfig.getConfigString("db_name").get
    val table_name = mysqlConfig.getConfigString("table_name").get

    val url = s"jdbc:mysql://${host}:${port}"
    val df = sqlContext.read.format("jdbc")
      .option("url", url)
      .option("user", mysqlConfig.getConfigString("user").get)
      .option("password", mysqlConfig.getConfigString("password").get)
      .option("dbtable", s"${db_name}.${table_name}")
      .load()
    Some(df)
  }
}
