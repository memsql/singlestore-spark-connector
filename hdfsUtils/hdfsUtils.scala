package com.memsql.hdfs_utils

import java.io.{IOException, FileNotFoundException}
import java.net.URI
import java.util.Properties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, GlobFilter}
import org.apache.hadoop.security.AccessControlException
import org.apache.log4j.PropertyConfigurator
import scala.collection.mutable.ListBuffer
import spray.json._

case class HDFSInfo(
  action: String = null,
  hdfs_server: String = null,
  user: String = null,
  path: String = null
)
case class HDFSFile(
  path: String,
  size: Long
)
case class HDFSLsResult(
  success: Boolean,
  files: List[HDFSFile]=List(),
  error: Option[String]=None
)

object JsonProto extends DefaultJsonProtocol {
  implicit val hdfsFileFormat = jsonFormat2(HDFSFile)
  implicit val hdfsLsResultFormat = jsonFormat3(HDFSLsResult)
}
import JsonProto._

object HDFSUtils {
  val VERSION = "0.0.1"

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("log4j.rootCategory", "INFO, console")
    props.setProperty("log4j.appender.console", "org.apache.log4j.varia.NullAppender")
    PropertyConfigurator.configure(props)

    val parser = new scopt.OptionParser[HDFSInfo]("hdfs-utils") {
      override def showUsageOnError = true

      head("hdfsUtils", HDFSUtils.VERSION)
      cmd("ls") action { (_, c) =>
        c.copy(action = "ls") } text("list files for a given path.") children(
        opt[String]("hdfs-server") required() action { (x, c) => c.copy(hdfs_server = x) },
        opt[String]("user") required() action { (x, c) => c.copy(user = x) },
        arg[String]("path") required() action { (x, c) => c.copy(path = x) }
      )
    }

    parser.parse(args, HDFSInfo()) match {
      case Some(info) => {
        if (info.action != "ls") {
          sys.exit(1)
        }
        val hadoopConf = new Configuration
        var result: HDFSLsResult = null
        try {
          val fileSystem = FileSystem.get(new URI(info.hdfs_server), hadoopConf, info.user)
          val fileStatusList = fileSystem.globStatus(new Path(info.path))
          var fileList = List[HDFSFile]()
          if (fileStatusList != null) {
            fileList = fileStatusList
              .filter(x => x.isFile)
              .map(x => HDFSFile(
                path = Path.getPathWithoutSchemeAndAuthority(x.getPath).toString,
                size = x.getLen
              ))
              .toList
          }
          result = HDFSLsResult(success = true, files = fileList)
        } catch {
          case e: AccessControlException => {
            result = HDFSLsResult(success = false, error = Some(s"${info.user} does not have permission to view ${info.path}"))
          }
          case e: Exception => {
            result = HDFSLsResult(success = false, error = Some(e.toString))
          }
        }
        println(result.toJson.prettyPrint)
        sys.exit(if (result.success) 0 else 1)
      }
      case None => sys.exit(1)
    }
  }
}
