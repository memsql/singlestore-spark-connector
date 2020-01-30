package com.memsql.spark

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

case class MemsqlOptions(
    masterHost: String,
    masterPort: Int,
    database: Option[String],
    user: String,
    password: Option[String],
    truncate: Boolean,
    loadDataCompression: MemsqlOptions.CompressionType.Value,
    jdbcExtraOptions: Map[String, String]
) {
  @transient lazy val masterConnectionInfo: PartitionConnectionInfo =
    PartitionConnectionInfo(
      host = masterHost,
      port = masterPort,
      database = database
    )
}

object MemsqlOptions {
  object CompressionType extends Enumeration {
    val GZip, LZ4, Skip = Value

    def fromString(s: String): Option[Value] = values.find(_.toString == s)
    lazy val valuesString                    = values.mkString(", ")
  }

  private val memsqlOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    memsqlOptionNames += name.toLowerCase()
    name
  }

  final val MASTER_HOST = newOption("masterHost")
  final val MASTER_PORT = newOption("masterPort")

  final val USER     = newOption("user")
  final val PASSWORD = newOption("password")

  final val DATABASE   = newOption("database")
  final val QUERY      = newOption("query")
  final val TABLE_NAME = newOption("dbtable")

  final val TRUNCATE              = newOption("truncate")
  final val LOAD_DATA_COMPRESSION = newOption("loadDataCompression")

  def getTable(options: CaseInsensitiveMap[String]): Option[TableIdentifier] =
    options
      .get(TABLE_NAME)
      .map(CatalystSqlParser.parseTableIdentifier)

  def getQuery(options: CaseInsensitiveMap[String]): String = {
    val table = getTable(options)

    require(
      !(options.isDefinedAt(TABLE_NAME) && options.isDefinedAt(QUERY)),
      s"Options '$TABLE_NAME' and '$QUERY' cannot both be specified."
    )

    options
      .get(QUERY)
      .orElse(table.map(t => s"SELECT * FROM ${t.quotedString}"))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Either the '$TABLE_NAME' or '$QUERY' option must be specified."
        )
      )
  }

  def apply(options: CaseInsensitiveMap[String]): MemsqlOptions = {
    val table = getTable(options)

    val required = (k: String) => require(options.isDefinedAt(k), s"Option '$k' is required.")
    required(MASTER_HOST)
    required(MASTER_PORT)

    val loadDataCompression = options
      .get(LOAD_DATA_COMPRESSION)
      .orElse(Some(CompressionType.GZip.toString))
      .flatMap(CompressionType.fromString)
      .getOrElse(
        sys.error(
          s"Option '$LOAD_DATA_COMPRESSION' must be one of the following values: ${CompressionType.valuesString}"
        )
      )

    new MemsqlOptions(
      masterHost = options(MASTER_HOST),
      masterPort = options.get(MASTER_PORT).map(_.toInt).get,
      database = options.get(DATABASE).orElse(table.flatMap(t => t.database)),
      user = options.get(USER).getOrElse("root"),
      password = options.get(PASSWORD),
      truncate = options.get(TRUNCATE).getOrElse("false").toBoolean,
      loadDataCompression = loadDataCompression,
      jdbcExtraOptions = options.originalMap
        .filterKeys(key => !memsqlOptionNames(key.toLowerCase()))
        // filterKeys produces a map which is not serializable due to a bug in Scala
        // https://github.com/scala/bug/issues/7005
        // mapping everything through the identity function fixes it...
        .map(identity)
    )
  }
}
