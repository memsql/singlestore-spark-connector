package com.memsql.sampling_utils

import com.memsql.spark.util.StringConversionUtils
import java.io.{BufferedReader, InputStreamReader, InputStream, PrintWriter, StringWriter}
import java.sql.{DriverManager, ResultSet}
import java.util.Properties
import org.apache.commons.csv._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDDHelper
import scala.collection.JavaConversions._
import spray.json._

case class S3Config(
  bucket: String = null,
  key: String = null,
  aws_access_key_id: String = null,
  aws_secret_access_key: String = null,
  delimiter: String = null,
  escape: Option[String] = None,
  quote: String = null,
  null_string: String = null,
  has_headers: Boolean = false
)

case class HDFSConfig(
  hdfs_server: String = null,
  hdfs_port: Int = -1,
  hdfs_user: String = null,
  path: String = null,
  delimiter: String = null,
  escape: Option[String] = None,
  quote: String = null,
  null_string: String = null,
  has_headers: Boolean = false
)

case class MySQLConfig(
  host: String = null,
  port: Int = -1,
  user: String = null,
  password: Option[String] = None,
  db_name: String = null,
  table_name: String = null
)

case class SamplingUtilsInfo(
  action: String = null,
  debug: Boolean = false,
  s3Config: S3Config = null,
  hdfsConfig: HDFSConfig = null,
  mysqlConfig: MySQLConfig = null
)
case class SamplingResult(
  success: Boolean,
  columns: Option[List[(String, String)]] = None,
  records: Option[List[List[JsValue]]] = None,
  error: Option[String] = None
)

object JsonProto extends DefaultJsonProtocol {
  implicit val samplingResultFormat = jsonFormat4(SamplingResult)
}
import JsonProto._

case class CSVSamplingException(message: String, lines: List[String]) extends Exception(message)

object SamplingUtils {
  val VERSION = "0.0.1"
  val SAMPLE_SIZE = 10
  val DEFAULT_JDBC_LOGIN_TIMEOUT = 10

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("log4j.rootCategory", "INFO, console")
    props.setProperty("log4j.appender.console", "org.apache.log4j.varia.NullAppender")
    PropertyConfigurator.configure(props)

    val parser = new scopt.OptionParser[SamplingUtilsInfo]("sampling-utils") {
      override def showUsageOnError = true

      head("samplingUtils", SamplingUtils.VERSION)
      opt[Unit]("debug") action { (_, c) => c.copy(debug = true) }
      cmd("s3") action { (_, c) =>
        c.copy(action = "s3", s3Config = S3Config()) } text("sample an S3 bucket.") children(
        opt[String]("bucket") required() action { (x, c) => c.copy(s3Config = c.s3Config.copy(bucket = x)) },
          opt[String]("key") required() action { (x, c) => c.copy(s3Config = c.s3Config.copy(key = x)) },
          opt[String]("aws-access-key-id") required() action { (x, c) => c.copy(s3Config = c.s3Config.copy(aws_access_key_id = x)) },
          opt[String]("aws-secret-access-key") required() action { (x, c) => c.copy(s3Config = c.s3Config.copy(aws_secret_access_key = x)) },
          opt[String]("delimiter") required() action { (x, c) => c.copy(s3Config = c.s3Config.copy(delimiter = x)) },
          opt[String]("escape") action { (x, c) => c.copy(s3Config = c.s3Config.copy(escape = Some(x))) },
          opt[String]("quote") required() action { (x, c) => c.copy(s3Config = c.s3Config.copy(quote = x)) },
          opt[String]("null-string") required() action { (x, c) => c.copy(s3Config = c.s3Config.copy(null_string = x)) },
          opt[Unit]("has-headers") action { (_, c) => c.copy(s3Config = c.s3Config.copy(has_headers = true)) }
      )

      cmd("hdfs") action { (_, c) =>
        c.copy(action = "hdfs", hdfsConfig = HDFSConfig()) } text("sample an HDFS directory") children(
        opt[String]("hdfs-server") required() action { (x, c) => c.copy(hdfsConfig = c.hdfsConfig.copy(hdfs_server = x)) },
          opt[Int]("hdfs-port") required() action { (x, c) => c.copy(hdfsConfig = c.hdfsConfig.copy(hdfs_port = x)) },
          opt[String]("hdfs-user") required() action { (x, c) => c.copy(hdfsConfig = c.hdfsConfig.copy(hdfs_user = x)) },
          opt[String]("path") required() action { (x, c) => c.copy(hdfsConfig = c.hdfsConfig.copy(path = x)) },
          opt[String]("delimiter") required() action { (x, c) => c.copy(hdfsConfig = c.hdfsConfig.copy(delimiter = x)) },
          opt[String]("escape") action { (x, c) => c.copy(hdfsConfig = c.hdfsConfig.copy(escape = Some(x))) },
          opt[String]("quote") required() action { (x, c) => c.copy(hdfsConfig = c.hdfsConfig.copy(quote = x)) },
          opt[String]("null-string") required() action { (x, c) => c.copy(hdfsConfig = c.hdfsConfig.copy(null_string = x)) },
          opt[Unit]("has-headers") action { (_, c) => c.copy(hdfsConfig = c.hdfsConfig.copy(has_headers = true)) }
      )

      cmd("mysql") action { (_, c) =>
        c.copy(action = "mysql", mysqlConfig = MySQLConfig()) } text("sample a MySQL table") children(
        opt[String]("host") required() action { (x, c) => c.copy(mysqlConfig = c.mysqlConfig.copy(host = x)) },
          opt[Int]("port") required() action { (x, c) => c.copy(mysqlConfig = c.mysqlConfig.copy(port = x)) },
          opt[String]("user") required() action { (x, c) => c.copy(mysqlConfig = c.mysqlConfig.copy(user = x)) },
          opt[String]("password") action { (x, c) => c.copy(mysqlConfig = c.mysqlConfig.copy(password = Some(x))) },
          opt[String]("db-name") required() action { (x, c) => c.copy(mysqlConfig = c.mysqlConfig.copy(db_name = x)) },
          opt[String]("table-name") required() action { (x, c) => c.copy(mysqlConfig = c.mysqlConfig.copy(table_name = x)) }
      )
    }

    parser.parse(args, SamplingUtilsInfo()) match {
      case Some(info) => {
        var result: SamplingResult = null
        try {
          if (info.action == "s3") {
            result = sampleS3(info)
          } else if (info.action == "hdfs") {
            result = sampleHDFS(info)
          } else if (info.action == "mysql") {
            result = sampleMySQL(info)
          } else {
            throw new IllegalArgumentException("Action not recognized")
          }
        } catch {
          case e: Exception => {
            val error = if (info.debug) {
              val sw = new StringWriter()
              e.printStackTrace(new PrintWriter(sw))
              sw.toString
            } else {
              e.getMessage
            }
            var lines: Option[List[List[JsValue]]] = None
            var columns: Option[List[(String,String)]] = None
            if (e.isInstanceOf[CSVSamplingException]) {
              lines = Some(e.asInstanceOf[CSVSamplingException].lines.map(l => List(JsString(l))))
              columns = Some(List(( "line", "string" )))
            }
            result = SamplingResult(success = false, error = Some(error), columns = columns, records = lines)
          }
        }
        if (result == null) {
          sys.exit(1)
        }
        println(result.toJson.prettyPrint)
        sys.exit(if (result.success) 0 else 1)
      }
      case None => sys.exit(1)
    }
  }

  def sampleS3(info: SamplingUtilsInfo): SamplingResult = {
    val hadoopConf = new Configuration
    hadoopConf.set("fs.s3n.awsAccessKeyId", info.s3Config.aws_access_key_id)
    hadoopConf.set("fs.s3n.awsSecretAccessKey", info.s3Config.aws_secret_access_key)
    hadoopConf.setBoolean("fs.s3n.impl.disable.cache", true)
    val bucket = info.s3Config.bucket
    val key = info.s3Config.key
    val path = s"s3n://${bucket}/${key}"

    val csvFormat = getCSVFormat(
      info.s3Config.delimiter, info.s3Config.quote, info.s3Config.escape)

    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(hadoopConf)

    sampleCSVFile(fsPath, fs, hadoopConf, csvFormat, info.s3Config.has_headers, info.s3Config.null_string)
  }

  def sampleHDFS(info: SamplingUtilsInfo): SamplingResult = {
    val hadoopConf = new Configuration
    hadoopConf.setBoolean("fs.hdfs.impl.disable.cache", true)
    val hdfsServer = info.hdfsConfig.hdfs_server
    val hdfsPort = info.hdfsConfig.hdfs_port
    val hdfsUser = info.hdfsConfig.hdfs_user
    val filePath = info.hdfsConfig.path
    val path = s"hdfs://${hdfsServer}:${hdfsPort}${filePath}"

    val csvFormat = getCSVFormat(
      info.hdfsConfig.delimiter, info.hdfsConfig.quote, info.hdfsConfig.escape)

    val fsPath = new Path(path)
    val fs = FileSystem.get(fsPath.toUri, hadoopConf, hdfsUser)

    sampleCSVFile(fsPath, fs, hadoopConf, csvFormat, info.hdfsConfig.has_headers, info.hdfsConfig.null_string)
  }

  def sampleMySQL(info: SamplingUtilsInfo): SamplingResult = {
    val host = info.mysqlConfig.host
    val port = info.mysqlConfig.port
    val user = info.mysqlConfig.user
    val password = info.mysqlConfig.password.getOrElse("")
    val dbName = info.mysqlConfig.db_name
    val tableName = info.mysqlConfig.table_name

    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val dbAddress = "jdbc:mysql://" + host + ":" + port + "/" + dbName
    DriverManager.setLoginTimeout(DEFAULT_JDBC_LOGIN_TIMEOUT)
    val conn = DriverManager.getConnection(dbAddress, user, password)

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", password)
    val schema = JDBCRDDHelper.resolveTable(dbAddress, tableName, properties)
    val columns = schema.fields.map(field => (field.name, field.dataType.typeName)).toList

    val query = s"SELECT * FROM `${tableName}` LIMIT ${SAMPLE_SIZE}"
    val stmt = conn.createStatement
    val rs = stmt.executeQuery(query)
    val metadata = rs.getMetaData
    val resultSetIterator = new Iterator[ResultSet] {
      def hasNext = rs.next()
      def next() = rs
    }
    val rows = resultSetIterator.take(SAMPLE_SIZE).map(rs => {
      (1 to metadata.getColumnCount).map(i => {
        val obj = rs.getObject(i)
        val stringValue = obj match {
          case null => "null"
          case bytes: Array[Byte] => StringConversionUtils.byteArrayToReadableString(bytes)
          case default => obj.toString
        }
        JsString(stringValue)
      }).toList
    }).toList
    SamplingResult(success = true, columns = Some(columns), records = Some(rows))
  }

  private def sampleCSVFile(path: Path, fs: FileSystem, hadoopConf: Configuration, csvFormat: CSVFormat, hasHeaders: Boolean, nullString: String): SamplingResult = {
    var fileIn: InputStream = null
    try {
      val factory = new CompressionCodecFactory(hadoopConf)
      val codec = factory.getCodec(path)
      fileIn = fs.open(path)
      val innerInputStream = if (codec != null) {
        codec.createInputStream(fileIn)
      } else {
        fileIn
      }
      val lines = scala.io.Source.fromInputStream(innerInputStream).getLines.take(SAMPLE_SIZE).toList

      try {
        val rows = lines.flatMap(l => CSVParser.parse(l, csvFormat).map(record => record.toList)).toList
        val firstRow = rows.head
        var columns: List[(String, String)] = null
        var records: List[List[String]] = null
        if (hasHeaders) {
          columns = firstRow.map(x => ( x, "string" ))
          records = rows.tail
        } else {
          columns = firstRow.zipWithIndex.map{ case(x, i) => ( s"column_${i + 1}", "string" ) }
          records = rows
        }

        rows.zipWithIndex.foreach{ case (row, i) => {
          if (row.size != firstRow.size) {
            throw new IllegalArgumentException(s"Row ${i + 1} has length ${row.size} but the first row has length ${firstRow.size}")
          }
        }}

        val jsValueRecords = records.map(record => record.map(x => {
          if (!nullString.isEmpty && x == nullString) {
            JsNull
          } else {
            JsString(x)
          }
        }))
        SamplingResult(success = true, columns = Some(columns), records = Some(jsValueRecords))
      } catch {
        case e: Exception => {
          throw new CSVSamplingException(e.getMessage, lines)
        }
      }
    } finally {
      if (fileIn != null) {
        try {
          fileIn.close()
        } catch {
          case e: Exception => //
        }
      }
    }
  }

  private def getCSVFormat(delimiter: String, quote: String, escape: Option[String]): CSVFormat = {
    if (delimiter.size != 1) {
      throw new IllegalArgumentException("Delimiter is not a single character")
    }
    if (quote.size != 1) {
      throw new IllegalArgumentException("Quote is not a single character")
    }
    val csvFormat = CSVFormat.newFormat(delimiter.head)
      .withIgnoreSurroundingSpaces()
      .withIgnoreEmptyLines(false)
      .withRecordSeparator('\n')
      .withQuote(quote.head)

    escape match {
      case None => csvFormat.withEscape('\\')
      case Some("") => csvFormat
      case Some(x) if x.size == 1 => csvFormat.withEscape(x.head)
      case _ => throw new IllegalArgumentException("Escape is not a single character")
    }
  }
}
