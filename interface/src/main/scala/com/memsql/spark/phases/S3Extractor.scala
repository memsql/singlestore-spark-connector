package com.memsql.spark.phases

import com.memsql.spark.etl.api.{Extractor, PhaseConfig}
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.StreamingContext


// Reading from a public S3 bucket without providing valid credentials is not yet supported.
case class S3ExtractTaskConfig(key: String)

case class S3ExtractConfig(aws_access_key_id: String,
                           aws_secret_access_key: String,
                           bucket: String,
                           task_config: S3ExtractTaskConfig,
                           max_records: Option[Int],
                           num_records_to_skip: Option[Int]) extends PhaseConfig

case class S3ExtractException(message: String) extends Exception(message)

class S3Extractor extends Extractor {
  override def next(ssc: StreamingContext,
                    time: Long,
                    sqlContext: SQLContext,
                    config: PhaseConfig,
                    batchInterval: Long,
                    logger: PhaseLogger): Option[DataFrame] = {

    val sc = ssc.sparkContext
    val s3config = config.asInstanceOf[S3ExtractConfig]

    val hadoopConf = new Configuration
    hadoopConf.set("fs.s3n.awsAccessKeyId", s3config.aws_access_key_id)
    hadoopConf.set("fs.s3n.awsSecretAccessKey", s3config.aws_secret_access_key)

    // Prevent org.apache.hadoop.fs.FileSystem from storing an instance of S3NativeFileSystem in its static CACHE,
    // which would cause every Spark run to use the same AWS credentials.
    hadoopConf.setBoolean("fs.s3n.impl.disable.cache", true)

    val bucket = s3config.bucket
    val key = s3config.task_config.key
    val path = s"s3n://${bucket}/${key}"

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
        throw S3ExtractException(s"Invalid bucket: ${bucket}")
      }
    }

    if (s3config.num_records_to_skip.isDefined) {
      rowRDD = rowRDD
        .zipWithIndex
        .filter(x => x._2 >= s3config.num_records_to_skip.get)
        .map(x => x._1)
    }
    if (s3config.max_records.isDefined) {
      rowRDD = sc.parallelize(rowRDD.take(s3config.max_records.get))
    }

    val schema = StructType(
      Seq(StructField("line", StringType, nullable = false)))

    val df = sqlContext.createDataFrame(rowRDD, schema)
    Some(df)
  }
}
