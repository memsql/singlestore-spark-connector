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
                              max_records: Option[Int],
                              task_config: MySQLExtractTaskConfig) extends PhaseConfig

class MySQLExtractor extends Extractor {
  override def next(ssc: StreamingContext,
                    time: Long,
                    sqlContext: SQLContext,
                    config: PhaseConfig,
                    batchInterval: Long,
                    logger: PhaseLogger): Option[DataFrame] = {
    val sc = ssc.sparkContext
    val mysqlConfig = config.asInstanceOf[MySQLExtractConfig]

    val host = mysqlConfig.host
    val port = mysqlConfig.port
    val db_name = mysqlConfig.db_name
    val table_name = mysqlConfig.table_name

    val url = s"jdbc:mysql://${host}:${port}"
    var df = sqlContext.read.format("jdbc")
      .option("url", url)
      .option("user", mysqlConfig.user)
      .option("password", mysqlConfig.password)
      .option("dbtable", s"${db_name}.${table_name}")
      .load()

    if (mysqlConfig.max_records.isDefined) {
      df = df.limit(mysqlConfig.max_records.get)
    }

    Some(df)
  }
}
