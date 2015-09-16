package com.memsql.spark.interface

import com.memsql.spark.interface.meta.BuildInfo

object Main {
  val VERSION = BuildInfo.version

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("memsql-spark-interface") {
      override def showUsageOnError = true

      head("MemSQL Spark Interface", VERSION)
      opt[Int]('P', "port") action { (x, c) => c.copy(port = x) } text "MemSQL Spark Interface port"
      opt[String]('D', "dataDir") required() action { (x, c) => c.copy(dataDir = x) } text "MemSQL Spark Interface data directory"

      opt[String]("dbHost") action { (x, c) => c.copy(dbHost = x) } text "MemSQL Master host"
      opt[Int]("dbPort") action { (x, c) => c.copy(dbPort = x) } text "MemSQL Master port"
      opt[String]("dbUser") action { (x, c) => c.copy(dbUser = x) } text "MemSQL Master user"
      opt[String]("dbPassword") action { (x, c) => c.copy(dbPassword = x) } text "MemSQL Master password"
      opt[String]("metadataDbName") action { (x, c) => c.copy(metadatadbName = x) } text "MemSQL Streamliner database for checkpoints and metadata"

      opt[Unit]("debug") action { (_, c) => c.copy(debug = true) } text "Enable debug logging"
    }

    parser.parse(args, Config()) match {
      case Some(config) => {
        new SparkInterface(config).run()
      }
      case None => sys.exit(1)
    }
  }
}
