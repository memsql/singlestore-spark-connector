package com.memsql.spark

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

case class MemsqlOptions(
    ddlEndpoint: String,
    dmlEndpoints: List[String],
    user: String,
    password: String,
    database: Option[String],
    truncate: Boolean,
    loadDataCompression: MemsqlOptions.CompressionType.Value,
    jdbcExtraOptions: Map[String, String],
    enableAsserts: Boolean,
    disablePushdown: Boolean,
    enableParallelRead: Boolean
) extends LazyLogging {

  def assert(condition: Boolean, message: String) = {
    if (!condition) {
      if (enableAsserts) {
        throw new AssertionError(message)
      } else {
        log.trace(s"assertion failed: ${message}")
      }
    }
  }
}

object MemsqlOptions {
  object CompressionType extends Enumeration {
    val GZip, LZ4, Skip = Value

    def fromString(s: String): Option[Value] =
      values.find(_.toString.toLowerCase() == s.toLowerCase())
    lazy val valuesString = values.mkString(", ")
  }

  private val memsqlOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    memsqlOptionNames += name.toLowerCase()
    name
  }

  final val DDL_ENDPOINT  = newOption("ddlEndpoint")
  final val DML_ENDPOINTS = newOption("dmlEndpoints")

  final val USER     = newOption("user")
  final val PASSWORD = newOption("password")

  final val DATABASE   = newOption("database")
  final val QUERY      = newOption("query")
  final val TABLE_NAME = newOption("dbtable")
  final val PATH       = newOption("path")

  final val TRUNCATE              = newOption("truncate")
  final val LOAD_DATA_COMPRESSION = newOption("loadDataCompression")

  final val ENABLE_ASSERTS       = newOption("enableAsserts")
  final val DISABLE_PUSHDOWN     = newOption("disablePushdown")
  final val ENABLE_PARALLEL_READ = newOption("enableParallelRead")

  def getTable(options: CaseInsensitiveMap[String]): Option[TableIdentifier] =
    options
      .get(TABLE_NAME)
      .orElse(options.get(PATH))
      .map(CatalystSqlParser.parseTableIdentifier)

  def getQuery(options: CaseInsensitiveMap[String]): String = {
    val table = getTable(options)

    require(
      !(table.isDefined && options.isDefinedAt(QUERY)),
      s"The '$QUERY' option cannot be specified along with a table name."
    )

    options
      .get(QUERY)
      .orElse(table.map(t => s"SELECT * FROM ${t.quotedString}"))
      .getOrElse(
        throw new IllegalArgumentException(
          s"One of the following options must be specified: $QUERY, $TABLE_NAME, $PATH"
        )
      )
  }

  def apply(options: CaseInsensitiveMap[String]): MemsqlOptions = {
    val table = getTable(options)

    require(options.isDefinedAt(DDL_ENDPOINT), s"Option '$DDL_ENDPOINT' is required.")

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
      ddlEndpoint = options(DDL_ENDPOINT),
      dmlEndpoints = options.getOrElse(DML_ENDPOINTS, options(DDL_ENDPOINT)).split(",").toList,
      user = options.getOrElse(USER, "root"),
      password = options.getOrElse(PASSWORD, ""),
      database = options.get(DATABASE).orElse(table.flatMap(t => t.database)),
      truncate = options.get(TRUNCATE).getOrElse("false").toBoolean,
      loadDataCompression = loadDataCompression,
      jdbcExtraOptions = options.originalMap
        .filterKeys(key => !memsqlOptionNames(key.toLowerCase()))
        // filterKeys produces a map which is not serializable due to a bug in Scala
        // https://github.com/scala/bug/issues/7005
        // mapping everything through the identity function fixes it...
        .map(identity),
      enableAsserts = options.get(ENABLE_ASSERTS).getOrElse("false").toBoolean,
      disablePushdown = options.get(DISABLE_PUSHDOWN).getOrElse("false").toBoolean,
      enableParallelRead = options.get(ENABLE_PARALLEL_READ).getOrElse("false").toBoolean
    )
  }
}
