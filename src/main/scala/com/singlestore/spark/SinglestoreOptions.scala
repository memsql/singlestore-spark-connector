package com.singlestore.spark

import com.singlestore.spark.SinglestoreOptions.TableKey
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

case class SinglestoreOptions(
    ddlEndpoint: String,
    dmlEndpoints: List[String],
    user: String,
    password: String,
    database: Option[String],
    jdbcExtraOptions: Map[String, String],
    enableAsserts: Boolean,
    // read options
    disablePushdown: Boolean,
    enableParallelRead: ParallelReadEnablement,
    parallelReadFeatures: List[ParallelReadType],
    parallelReadTableCreationTimeoutMS: Long,
    parallelReadMaterializedTableCreationTimeoutMS: Long,
    parallelReadMaxNumPartitions: Int,
    parallelReadRepartition: Boolean,
    parallelReadRepartitionColumns: Set[String],
    // write options
    overwriteBehavior: OverwriteBehavior,
    loadDataCompression: SinglestoreOptions.CompressionType.Value,
    loadDataFormat: SinglestoreOptions.LoadDataFormat.Value,
    tableKeys: List[TableKey],
    onDuplicateKeySQL: Option[String],
    maxErrors: Int,
    insertBatchSize: Int,
    createRowstoreTable: Boolean,
    driverConnectionPoolOptions: SinglestoreConnectionPoolOptions,
    executorConnectionPoolOptions: SinglestoreConnectionPoolOptions
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

object SinglestoreOptions extends LazyLogging {
  abstract class OptionEnum extends Enumeration {
    def fromString(s: String): Option[Value] =
      values.find(_.toString.toLowerCase() == s.toLowerCase())

    lazy val valuesString = values.mkString(", ")
    def fromOption(optionName: String, source: Option[String], default: Value): Value =
      source
        .orElse(Some(default.toString))
        .flatMap(fromString)
        .getOrElse(
          sys.error(
            s"Option '$optionName' must be one of the following values: ${valuesString}"
          )
        )
  }

  object LoadDataFormat extends OptionEnum {
    val CSV, Avro = Value
  }

  object CompressionType extends OptionEnum {
    val GZip, LZ4, Skip = Value
  }

  object TableKeyType extends OptionEnum {
    val Primary, Columnstore, Unique, Shard, Key = Value
  }

  case class TableKey(keyType: TableKeyType.Value,
                      name: Option[String] = None,
                      columns: String = "")

  private val singlestoreOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    singlestoreOptionNames += name.toLowerCase()
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

  // Write options
  final val TRUNCATE               = newOption("truncate")
  final val OVERWRITE_BEHAVIOR     = newOption("overwriteBehavior")
  final val PARALLEL_READ_FEATURES = newOption("parallelRead.features")
  final val PARALLEL_READ_TABLE_CREATION_TIMEOUT_MS = newOption(
    "parallelRead.tableCreationTimeoutMS")
  final val PARALLEL_READ_MATERIALIZED_TABLE_CREATION_TIMEOUT_MS = newOption(
    "parallelRead.materializedTableCreationTimeoutMS")
  final val PARALLEL_READ_REPARTITION         = newOption("parallelRead.repartition")
  final val PARALLEL_READ_REPARTITION_COLUMNS = newOption("parallelRead.repartition.columns")
  final val PARALLEL_READ_MAX_NUM_PARTITIONS  = newOption("parallelRead.maxNumPartitions")

  final val LOAD_DATA_COMPRESSION = newOption("loadDataCompression")
  final val TABLE_KEYS            = newOption("tableKey")
  final val LOAD_DATA_FORMAT      = newOption("loadDataFormat")
  final val ON_DUPLICATE_KEY_SQL  = newOption("onDuplicateKeySQL")
  final val INSERT_BATCH_SIZE     = newOption("insertBatchSize")
  final val MAX_ERRORS            = newOption("maxErrors")
  final val CREATE_ROWSTORE_TABLE = newOption("createRowstoreTable")

  final val ENABLE_ASSERTS       = newOption("enableAsserts")
  final val DISABLE_PUSHDOWN     = newOption("disablePushdown")
  final val ENABLE_PARALLEL_READ = newOption("enableParallelRead")

  final val DRIVER_CONNECTION_POOL_ENABLED        = newOption("driverConnectionPool.Enabled")
  final val DRIVER_CONNECTION_POOL_MAX_OPEN_CONNS = newOption("driverConnectionPool.MaxOpenConns")
  final val DRIVER_CONNECTION_POOL_MAX_IDLE_CONNS = newOption("driverConnectionPool.MaxIdleConns")
  final val DRIVER_CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME_MS = newOption(
    "driverConnectionPool.MinEvictableIdleTimeMS")
  final val DRIVER_CONNECTION_POOL_TIME_BETWEEN_EVICTION_RUNS_MS = newOption(
    "driverConnectionPool.TimeBetweenEvictionRunsMS")
  final val DRIVER_CONNECTION_POOL_MAX_WAIT_MS = newOption("driverConnectionPool.MaxWaitMS")
  final val DRIVER_CONNECTION_POOL_MAX_CONN_LIFETIME_MS = newOption(
    "driverConnectionPool.MaxConnLifetimeMS")

  final val EXECUTOR_CONNECTION_POOL_ENABLED = newOption("executorConnectionPool.Enabled")
  final val EXECUTOR_CONNECTION_POOL_MAX_OPEN_CONNS = newOption(
    "executorConnectionPool.MaxOpenConns")
  final val EXECUTOR_CONNECTION_POOL_MAX_IDLE_CONNS = newOption(
    "executorConnectionPool.MaxIdleConns")
  final val EXECUTOR_CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME_MS = newOption(
    "executorConnectionPool.MinEvictableIdleTimeMS")
  final val EXECUTOR_CONNECTION_POOL_TIME_BETWEEN_EVICTION_RUNS_MS = newOption(
    "executorConnectionPool.TimeBetweenEvictionRunsMS")
  final val EXECUTOR_CONNECTION_POOL_MAX_WAIT_MS = newOption("executorConnectionPool.MaxWaitMS")
  final val EXECUTOR_CONNECTION_POOL_MAX_CONN_LIFETIME_MS = newOption(
    "executorConnectionPool.MaxConnLifetimeMS")

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

  def splitEscapedColumns(s: String): List[String] = {
    def splitEscapedColumns(s: String, isQuoteOpen: Boolean): List[String] = {
      if (s.isEmpty) {
        List.empty
      } else if (s.head == '`') {
        val suffixRes = splitEscapedColumns(s.tail, !isQuoteOpen)
        if (suffixRes.isEmpty) {
          // it is the first column that we meet
          // return list with one string
          List("`")
        } else {
          // prepend character to the first column
          ('`' + suffixRes.head) +: suffixRes.tail
        }
      } else if (s.head == ',' && !isQuoteOpen) {
        val res = splitEscapedColumns(s.tail, isQuoteOpen)
        // we are processing a new column
        // prepend empty column to the result
        "" +: res
      } else {
        val res = splitEscapedColumns(s.tail, isQuoteOpen)
        if (res.isEmpty) {
          // it is the first column that we meet
          // return list with one string
          List(s.head.toString)
        } else {
          // prepend character to the first column
          (s.head + res.head) +: res.tail
        }
      }
    }

    splitEscapedColumns(s, isQuoteOpen = false)
  }

  def trimAndUnescapeColumn(s: String): String = {
    def unescapeColumn(s: List[Char]): List[Char] = {
      if (s.isEmpty) {
        List.empty
      } else if (s.head == '`' && s.tail.nonEmpty && s.tail.head == '`') {
        // we met escaped backtick
        // append it only once
        '`' +: unescapeColumn(s.tail.tail)
      } else if (s.head == '`') {
        // we met backtick
        // skip it
        unescapeColumn(s.tail)
      } else {
        // add a simple character
        s.head +: unescapeColumn(s.tail)
      }
    }

    val trimmed = s.trim
    if (trimmed.nonEmpty && trimmed.head == '`') {
      unescapeColumn(s.trim.toCharArray.toList).mkString("")
    } else {
      trimmed
    }
  }

  def apply(options: CaseInsensitiveMap[String]): SinglestoreOptions = {
    val table = getTable(options)

    require(options.isDefinedAt(DDL_ENDPOINT), s"Option '$DDL_ENDPOINT' is required.")

    val loadDataCompression = CompressionType.fromOption(LOAD_DATA_COMPRESSION,
                                                         options.get(LOAD_DATA_COMPRESSION),
                                                         CompressionType.GZip)

    // tableKeys are specified via options with the tableKey prefix
    // the key is `tableKey.TYPE.NAME` where
    //    TYPE is one of "primary, unique, shard, index"
    //    NAME is the index name - must be unique
    // the value is a column delimited list of columns for the index
    val tableKeys = options
      .filterKeys(_.toLowerCase.startsWith(TABLE_KEYS.toLowerCase + "."))
      .map {
        case (key, columns) => {
          val keyParts = key.split("\\.", 3)
          if (keyParts.length < 2) {
            sys.error(
              s"Options starting with '$TABLE_KEYS.' must be formatted correctly. The key should be in the form `$TABLE_KEYS.INDEX_TYPE[.NAME]`."
            )
          }
          val keyType = TableKeyType
            .fromString(keyParts(1))
            .getOrElse(
              sys.error(
                s"Option '$key' must specify an index type from the following options: ${TableKeyType.valuesString}")
            )
          val keyName = keyParts.lift(2)
          if (keyName.contains("")) {
            sys.error(s"Option '$key' can not have an empty name")
          }
          TableKey(keyType, keyName, columns)
        }
      }
      .toList

    new SinglestoreOptions(
      ddlEndpoint = options(DDL_ENDPOINT),
      dmlEndpoints =
        options.getOrElse(DML_ENDPOINTS, options(DDL_ENDPOINT)).split(",").toList.sorted,
      user = options.getOrElse(USER, "root"),
      password = options.getOrElse(PASSWORD, ""),
      database = options.get(DATABASE).orElse(table.flatMap(t => t.database)),
      jdbcExtraOptions = options.originalMap
        .filterKeys(key => !singlestoreOptionNames(key.toLowerCase()))
        // filterKeys produces a map which is not serializable due to a bug in Scala
        // https://github.com/scala/bug/issues/7005
        // mapping everything through the identity function fixes it...
        .map(identity),
      enableAsserts = options.get(ENABLE_ASSERTS).getOrElse("false").toBoolean,
      disablePushdown = options.get(DISABLE_PUSHDOWN).getOrElse("false").toBoolean,
      enableParallelRead =
        ParallelReadEnablement(options.get(ENABLE_PARALLEL_READ).getOrElse("automaticLite")),
      overwriteBehavior = {
        val truncateOption          = options.get(TRUNCATE)
        val overwriteBehaviorOption = options.get(OVERWRITE_BEHAVIOR)
        if (truncateOption.isDefined && overwriteBehaviorOption.isDefined) {
          throw new IllegalArgumentException(
            s"can't use both `$TRUNCATE` and `$OVERWRITE_BEHAVIOR` options, please use just `$OVERWRITE_BEHAVIOR` option instead.")
        }
        if (truncateOption.getOrElse("false").toBoolean) {
          log.warn(
            s"`$TRUNCATE` option is deprecated, please use the `$OVERWRITE_BEHAVIOR` option instead.")
          Truncate
        } else {
          /* DropAndCreate is the default behaviour if another isn't defined */
          overwriteBehaviorOption
            .fold[OverwriteBehavior](DropAndCreate)(OverwriteBehavior(_))
        }
      },
      loadDataCompression = loadDataCompression,
      loadDataFormat = LoadDataFormat.fromOption(LOAD_DATA_FORMAT,
                                                 options.get(LOAD_DATA_FORMAT),
                                                 LoadDataFormat.CSV),
      tableKeys = tableKeys,
      onDuplicateKeySQL = options.get(ON_DUPLICATE_KEY_SQL),
      insertBatchSize = options.get(INSERT_BATCH_SIZE).getOrElse("10000").toInt,
      maxErrors = {
        val onDuplicateKeySqlOption = options.get(ON_DUPLICATE_KEY_SQL)
        val maxErrorsOption         = options.get(MAX_ERRORS)
        if (maxErrorsOption.isDefined && onDuplicateKeySqlOption.isDefined) {
          throw new IllegalArgumentException(
            s"can't use both `$ON_DUPLICATE_KEY_SQL` and `$MAX_ERRORS` options")
        } else {
          maxErrorsOption.getOrElse("0").toInt
        }
      },
      parallelReadFeatures = {
        options
          .get(PARALLEL_READ_FEATURES)
          .getOrElse("ReadFromAggregators")
          .split(",")
          .map(feature => ParallelReadType(feature.trim))
          .toList
      },
      parallelReadTableCreationTimeoutMS = {
        options.getOrElse(PARALLEL_READ_TABLE_CREATION_TIMEOUT_MS, "0").toInt
      },
      parallelReadMaterializedTableCreationTimeoutMS = {
        options.getOrElse(PARALLEL_READ_MATERIALIZED_TABLE_CREATION_TIMEOUT_MS, "0").toInt
      },
      parallelReadMaxNumPartitions = options.getOrElse(PARALLEL_READ_MAX_NUM_PARTITIONS, "0").toInt,
      parallelReadRepartition = options.get(PARALLEL_READ_REPARTITION).getOrElse("false").toBoolean,
      parallelReadRepartitionColumns =
        splitEscapedColumns(options.get(PARALLEL_READ_REPARTITION_COLUMNS).getOrElse(""))
          .map(column => trimAndUnescapeColumn(column))
          .toSet,
      createRowstoreTable = options.getOrElse(CREATE_ROWSTORE_TABLE, "false").toBoolean,
      executorConnectionPoolOptions = SinglestoreConnectionPoolOptions(
        options.getOrElse(EXECUTOR_CONNECTION_POOL_ENABLED, "true").toBoolean,
        options.getOrElse(EXECUTOR_CONNECTION_POOL_MAX_OPEN_CONNS, "-1").toInt,
        options.getOrElse(EXECUTOR_CONNECTION_POOL_MAX_IDLE_CONNS, "8").toInt,
        options.getOrElse(EXECUTOR_CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME_MS, "30000").toLong,
        options.getOrElse(EXECUTOR_CONNECTION_POOL_TIME_BETWEEN_EVICTION_RUNS_MS, "1000").toLong,
        options.getOrElse(EXECUTOR_CONNECTION_POOL_MAX_WAIT_MS, "-1").toLong,
        options.getOrElse(EXECUTOR_CONNECTION_POOL_MAX_CONN_LIFETIME_MS, "-1").toLong,
      ),
      driverConnectionPoolOptions = SinglestoreConnectionPoolOptions(
        options.getOrElse(DRIVER_CONNECTION_POOL_ENABLED, "true").toBoolean,
        options.getOrElse(DRIVER_CONNECTION_POOL_MAX_OPEN_CONNS, "-1").toInt,
        options.getOrElse(DRIVER_CONNECTION_POOL_MAX_IDLE_CONNS, "8").toInt,
        options.getOrElse(DRIVER_CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME_MS, "2000").toLong,
        options.getOrElse(DRIVER_CONNECTION_POOL_TIME_BETWEEN_EVICTION_RUNS_MS, "1000").toLong,
        options.getOrElse(DRIVER_CONNECTION_POOL_MAX_WAIT_MS, "-1").toLong,
        options.getOrElse(DRIVER_CONNECTION_POOL_MAX_CONN_LIFETIME_MS, "-1").toLong,
      ),
    )
  }
}
