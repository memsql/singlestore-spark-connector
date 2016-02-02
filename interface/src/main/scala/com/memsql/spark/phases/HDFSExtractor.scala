package com.memsql.spark.phases

import com.memsql.spark.etl.api.{Extractor, PhaseConfig}
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.StreamingContext


case class HDFSExtractTaskConfig(path: String)

case class HDFSExtractConfig(hdfs_server: String,
                             hdfs_port: Int,
                             task_config: HDFSExtractTaskConfig,
                             max_records: Option[Int]) extends PhaseConfig

case class HDFSExtractException(message: String) extends Exception(message)

class HDFSExtractor extends Extractor {
  override def next(ssc: StreamingContext,
                    time: Long,
                    sqlContext: SQLContext,
                    config: PhaseConfig,
                    batchInterval: Long,
                    logger: PhaseLogger): Option[DataFrame] = {

    val sc = ssc.sparkContext
    val hdfsConfig = config.asInstanceOf[HDFSExtractConfig]

    val hadoopConf = new Configuration
    hadoopConf.setBoolean("fs.hdfs.impl.disable.cache", true)

    val hdfsServer = hdfsConfig.hdfs_server
    val hdfsPort = hdfsConfig.hdfs_port
    val filePath = hdfsConfig.task_config.path
    val path = s"hdfs://${hdfsServer}:${hdfsPort}${filePath}"

    var rowRDD = try {
      sc.newAPIHadoopFile(
        path,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text],
        hadoopConf
      ).map(tuple => Row(tuple._2.toString))
    } catch {
      case e: IllegalArgumentException if e.getMessage contains "Invalid hostname in URI" => {
        throw HDFSExtractException(s"Invalid HDFS host: ${hdfsServer}")
      }
    }

    if (hdfsConfig.max_records.isDefined) {
      rowRDD = sc.parallelize(rowRDD.take(hdfsConfig.max_records.get))
    }

    val schema = StructType(
      Seq(StructField("line", StringType, nullable = false)))

    val df = sqlContext.createDataFrame(rowRDD, schema)
    Some(df)
  }
}
