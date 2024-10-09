package com.singlestore.spark

import java.sql.{
  Connection,
  PreparedStatement,
  SQLException,
  SQLInvalidAuthorizationSpecException,
  Statement
}
import java.util.Properties
import java.util.UUID.randomUUID

import com.singlestore.spark.SinglestoreOptions.{TableKey, TableKeyType}
import com.singlestore.spark.SQLGen.{SinglestoreVersion, StringVar, VariableList}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.{StringType, StructType}

import scala.util.{Failure, Success, Try}
import org.apache.spark.DataSourceTelemetryHelpers

case class SinglestorePartitionInfo(ordinal: Int, name: String, hostport: String)

object JdbcHelpers extends LazyLogging with DataSourceTelemetryHelpers {
  final val SINGLESTORE_CONNECT_TIMEOUT = "10000" // 10 seconds in ms

  // register the SingleStoreDialect
  JdbcDialects.registerDialect(SinglestoreDialect)

  // Connection implicits
  implicit class ConnectionHelpers(val conn: Connection) {
    def withStatement[T](handle: Statement => T): T =
      Loan(conn.createStatement()).to(handle)

    def withPreparedStatement[T](query: String, handle: PreparedStatement => T): T =
      Loan(conn.prepareStatement(query)).to(handle)
  }

  def getConnProperties(conf: SinglestoreOptions,
                        isOnExecutor: Boolean,
                        hostPorts: String*): Properties = {
    val url: String = {
      val base = s"jdbc:singlestore:loadbalance://${hostPorts.mkString(",")}"
      conf.database match {
        case Some(d) => s"$base/$d"
        case None    => base
      }
    }

    val sessionVariables = Seq(
      "collation_server=utf8_general_ci",
      "sql_select_limit=18446744073709551615",
      "compile_only=false",
      "sql_mode='STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY'"
    ).mkString(",")

    val properties = new Properties()
    properties.setProperty("url", url)
    properties.setProperty("driverClassName", "com.singlestore.jdbc.Driver")
    properties.setProperty("username", conf.user)
    properties.setProperty("password", conf.password)
    properties.setProperty("connectionAttributes", s"_connector_name:SingleStore Spark Connector,_connector_version:${BuildInfo.version},_product_version:${conf.sparkVersion}")
    properties.setProperty(
      "connectionProperties",
      (Map(
        JDBCOptions.JDBC_TABLE_NAME -> "XXX",
        "zeroDateTimeBehavior"      -> "convertToNull",
        "allowLoadLocalInfile"      -> "true",
        "connectTimeout"            -> SINGLESTORE_CONNECT_TIMEOUT,
        "sessionVariables"          -> sessionVariables,
        "tinyInt1isBit"             -> "false",
        "allowLocalInfile"          -> "true"
      ) ++ conf.jdbcExtraOptions)
        .map(pair => pair._1 + "=" + pair._2)
        .mkString(";")
    )

    // This property is ignored by DBCP during the connection creation
    // It is needed, to have different connection pools for the driver and executor in a local mode
    properties.setProperty("isOnSparkExecutor", isOnExecutor.toString)

    val connectionPoolOptions = if (isOnExecutor) {
      conf.executorConnectionPoolOptions
    } else {
      conf.driverConnectionPoolOptions
    }

    if (connectionPoolOptions.enabled) {
      properties.setProperty("maxTotal", connectionPoolOptions.MaxOpenConns.toString)
      properties.setProperty("maxIdle", connectionPoolOptions.MaxIdleConns.toString)
      properties.setProperty("maxWaitMillis", connectionPoolOptions.MaxWaitMS.toString)
      properties.setProperty("minEvictableIdleTimeMillis",
                             connectionPoolOptions.MinEvictableIdleTimeMs.toString)
      properties.setProperty("maxConnLifetimeMillis",
                             connectionPoolOptions.MaxConnLifetimeMS.toString)
      properties.setProperty("timeBetweenEvictionRunsMillis",
                             connectionPoolOptions.TimeBetweenEvictionRunsMS.toString)
    }

    properties
  }

  def getDDLConnProperties(conf: SinglestoreOptions, isOnExecutor: Boolean): Properties =
    getConnProperties(conf, isOnExecutor, conf.ddlEndpoint)

  def getDMLConnProperties(conf: SinglestoreOptions, isOnExecutor: Boolean): Properties =
    getConnProperties(conf, isOnExecutor, conf.dmlEndpoints: _*)

  def executeQuery(conn: Connection, query: String, variables: Any*): Iterator[Row] = {
    val statement = conn.prepareStatement(query)
    try {
      fillStatementJdbc(statement, variables.toList)
      if (!statement.execute()) {
        // We don't have a ResultSet
        // Return an empty iterator
        Iterator[Row]()
      } else {
        val rs     = statement.getResultSet
        val schema = JdbcUtils.getSchema(rs, SinglestoreDialect, alwaysNullable = true)
        JdbcUtils.resultSetToRows(rs, schema)
      }
    } finally {
      statement.close()
    }
  }

  def loadSchema(conf: SinglestoreOptions, query: String, variables: VariableList): StructType = {
    val conn =
      SinglestoreConnectionPool.getConnection(getDDLConnProperties(conf, isOnExecutor = false))
    try {
      val statement =
        conn.prepareStatement(appendTagsToQuery(conf, SinglestoreDialect.getSchemaQuery(s"($query) AS q")))
      try {
        fillStatement(statement, variables)
        val rs = statement.executeQuery()
        try {
          JdbcUtils.getSchema(rs, SinglestoreDialect, alwaysNullable = true)
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  def explainQuery(conf: SinglestoreOptions, query: String, variables: VariableList): String = {
    val conn =
      SinglestoreConnectionPool.getConnection(getDDLConnProperties(conf, isOnExecutor = false))
    try {
      val statement = conn.prepareStatement(appendTagsToQuery(conf, s"EXPLAIN $query"))
      try {
        fillStatement(statement, variables)
        val rs = statement.executeQuery()
        try {
          var out = List.empty[String]
          while (rs.next) {
            out = rs.getString(1) :: out
          }
          out.reverseIterator.mkString("\n")
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  // explainJSONQuery runs `EXPLAIN JSON` on the query and returns the String
  // representing this queries plan as JSON.
  def explainJSONQuery(conf: SinglestoreOptions, query: String, variables: VariableList): String = {
    val conn =
      SinglestoreConnectionPool.getConnection(getDDLConnProperties(conf, isOnExecutor = false))
    try {
      val statement = conn.prepareStatement(appendTagsToQuery(conf, s"EXPLAIN JSON $query"))
      try {
        fillStatement(statement, variables)
        val rs = statement.executeQuery()
        try {
          // we only expect one row in the output
          if (!rs.next()) { assert(false, "EXPLAIN JSON failed to return a row") }
          val json = rs.getString(1)
          assert(!rs.next(), "EXPLAIN JSON returned more than one row")
          json
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  // partitionHostPorts returns a list of (ordinal, name, host:port) for all master
  // partitions in the specified database
  def partitionHostPorts(conf: SinglestoreOptions,
                         database: String): List[SinglestorePartitionInfo] = {
    val conn =
      SinglestoreConnectionPool.getConnection(getDDLConnProperties(conf, isOnExecutor = false))
    val sql = prepareAndLogSql(
      conf,
      s"""
        |SELECT HOST, PORT
        |FROM INFORMATION_SCHEMA.DISTRIBUTED_PARTITIONS
        |WHERE DATABASE_NAME = ? AND ROLE = "Master"
        |ORDER BY ORDINAL ASC;
        |""".stripMargin
    )
    try {
      val statement = conn.prepareStatement(sql)
      try {
        fillStatement(statement, List(StringVar(database)))
        val rs = statement.executeQuery()
        try {
          var out = List.empty[SinglestorePartitionInfo]
          var idx = 0
          while (rs.next) {
            out = SinglestorePartitionInfo(idx,
                                           s"${database}_${idx}",
                                           s"${rs.getString(1)}:${rs.getInt(2)}") :: out
            idx += 1
          }
          out.reverse
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  /**
    * Return map from original host/port to external host/port
    * @param conf options
    * @return Map `host:port` -> `externalHost:externalPort`
    */
  def externalHostPorts(conf: SinglestoreOptions): Map[String, String] = {
    val conn =
      SinglestoreConnectionPool.getConnection(getDDLConnProperties(conf, isOnExecutor = false))
    val sql = prepareAndLogSql(
      conf,
      s"""
        |SELECT IP_ADDR,
        |PORT,
        |EXTERNAL_HOST,
        |EXTERNAL_PORT
        |FROM INFORMATION_SCHEMA.MV_NODES
        |WHERE TYPE = "LEAF";
        |""".stripMargin
    )
    try {
      val statement = conn.prepareStatement(sql)
      try {
        val rs = statement.executeQuery()
        try {
          var out = Map.empty[String, String]
          while (rs.next) {
            val host               = rs.getString(1)
            val port               = rs.getInt(2)
            val externalHost       = rs.getString(3)
            val externalPortString = rs.getString(4)
            if (externalHost != null && externalPortString != null) {
              val externalPort = externalPortString.toInt
              out = out + (s"$host:$port" -> s"$externalHost:$externalPort")
            }
          }
          out
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  def fillStatement(stmt: PreparedStatement, variables: VariableList): Unit = {
    import SQLGen._
    if (variables.isEmpty) { return }

    variables.zipWithIndex.foreach {
      case (StringVar(v), index) => stmt.setString(index + 1, v)
      case (IntVar(v), index)    => stmt.setInt(index + 1, v)
      case (LongVar(v), index)   => stmt.setLong(index + 1, v)
      case (ShortVar(v), index)  => stmt.setShort(index + 1, v)
      case (FloatVar(v), index)  => stmt.setFloat(index + 1, v)
      case (DoubleVar(v), index) => stmt.setDouble(index + 1, v)
      case (DecimalVar(v), index) =>
        stmt.setBigDecimal(index + 1, v.toJavaBigDecimal)
      case (BooleanVar(v), index)   => stmt.setBoolean(index + 1, v)
      case (ByteVar(v), index)      => stmt.setByte(index + 1, v)
      case (DateVar(v), index)      => stmt.setDate(index + 1, v)
      case (TimestampVar(v), index) => stmt.setTimestamp(index + 1, v)
      case (v, _) =>
        throw new IllegalArgumentException(
          "Unexpected Variable Type: " + v.getClass.getName
        )
    }
  }

  def fillStatementJdbc(stmt: PreparedStatement, variables: List[Any]): Unit = {
    // here we leave it to JDBC driver to do type conversions
    if (variables.isEmpty) { return }
    for ((v, index) <- variables.zipWithIndex) {
      stmt.setObject(index + 1, v)
    }
  }

  def schemaToString(schema: StructType,
                     tableKeys: List[TableKey],
                     createRowstoreTable: Boolean): String = {
    // spark should never call any of our code if the schema is empty
    assert(schema.length > 0)

    val fieldsSql = schema.fields
      .map(field => {
        val name = SinglestoreDialect.quoteIdentifier(field.name)
        val typ = SinglestoreDialect
          .getJDBCType(field.dataType)
          .getOrElse(
            throw new IllegalArgumentException(
              s"Can't get JDBC type for ${field.dataType.simpleString}"
            )
          )
        val nullable  = if (field.nullable) "" else " NOT NULL"
        val collation = if (field.dataType == StringType) " COLLATE UTF8_BIN" else ""
        s"${name} ${typ.databaseTypeDefinition}${collation}${nullable}"
      })

    // we want to default all tables to columnstore, but in 6.8 and below you *must*
    // specify a sort key so we just pick the first column arbitrarily for now
    var finalTableKeys = tableKeys
    // if all the keys are shard keys it means there are no other keys so we can default
    if (!createRowstoreTable && tableKeys.forall(_.keyType == TableKeyType.Shard)) {
      finalTableKeys = TableKey(TableKeyType.Columnstore, columns = schema.head.name) :: tableKeys
    }

    def keyNameColumnsSQL(key: TableKey) =
      s"${key.name.map(SinglestoreDialect.quoteIdentifier).getOrElse("")}(${key.columns})"

    val keysSql = finalTableKeys.map {
      case key @ TableKey(TableKeyType.Primary, _, _) => s"PRIMARY KEY ${keyNameColumnsSQL(key)}"
      case key @ TableKey(TableKeyType.Columnstore, _, _) =>
        s"KEY ${keyNameColumnsSQL(key)} USING CLUSTERED COLUMNSTORE"
      case key @ TableKey(TableKeyType.Unique, _, _) => s"UNIQUE KEY ${keyNameColumnsSQL(key)}"
      case key @ TableKey(TableKeyType.Shard, _, _)  => s"SHARD KEY ${keyNameColumnsSQL(key)}"
      case key @ TableKey(TableKeyType.Key, _, _)    => s"KEY ${keyNameColumnsSQL(key)}"
    }

    (fieldsSql ++ keysSql).mkString("(\n  ", ",\n  ", "\n)")
  }

  def tableExists(conf: SinglestoreOptions, conn: Connection, table: TableIdentifier): Boolean = {
    conn.withStatement(
      stmt =>
        Try {
          try {
            stmt.execute(
              appendTagsToQuery(conf, SinglestoreDialect.getTableExistsQuery(table.quotedString))
            )
          } finally {
            stmt.close()
          }
        }.isSuccess
    )
  }

  def getSinglestoreVersion(conf: SinglestoreOptions): String = {
    val conn =
      SinglestoreConnectionPool.getConnection(getDDLConnProperties(conf, isOnExecutor = false))
    val sql = prepareAndLogSql(conf, "select @@memsql_version")
    val resultSet = conn.withStatement(stmt => {
      try {
        stmt.executeQuery(sql)
      } catch {
        case _: SQLException => throw new IllegalArgumentException("Can't get SingleStore version")
      } finally {
        stmt.close()
        conn.close()
      }
    })
    if (resultSet.next()) {
      resultSet.getString("@@memsql_version")
    } else throw new IllegalArgumentException("Can't get SingleStore version")
  }

  def createTable(conf: SinglestoreOptions,
                  conn: Connection,
                  table: TableIdentifier,
                  schema: StructType,
                  tableKeys: List[TableKey],
                  createRowstoreTable: Boolean,
                  version: SinglestoreVersion): Unit = {
    val sql = prepareAndLogSql(
      conf,
      s"CREATE ${if (createRowstoreTable && version.atLeast("7.3.0")) "ROWSTORE" else ""} TABLE ${table.quotedString} ${schemaToString(schema, tableKeys, createRowstoreTable)}"
    )
    conn.withStatement(stmt => stmt.executeUpdate(sql))
  }

  def getPartitionsCount(conn: Connection, database: String): Int = {
    val sql =
      s"SELECT num_partitions FROM information_schema.DISTRIBUTED_DATABASES WHERE database_name = '$database'"
    log.info(logEventNameTagger(s"Executing SQL:\n$sql"))
    val resultSet = conn.withStatement(stmt => stmt.executeQuery(sql))

    if (resultSet.next()) {
      resultSet.getInt("num_partitions")
    } else {
      throw new IllegalArgumentException(
        s"Failed to get number of partitions for '$database' database")
    }
  }

  def getResultTableName(applicationId: String,
                         stageId: Int,
                         rddId: Int,
                         attemptNumber: Int,
                         randHex: String): String = {
    s"rt_${applicationId.replace("-", "")}_${stageId}_${rddId}_${attemptNumber}_${randHex}"
  }

  def getCreateResultTableQuery(tableName: String,
                                query: String,
                                schema: StructType,
                                materialized: Boolean,
                                needsRepartition: Boolean,
                                repartitionColumns: Seq[String]): String = {
    val materializedStr = { if (materialized) { "MATERIALIZED" } else "" }
    if (needsRepartition) {
      if (repartitionColumns.isEmpty) {
        val randColName = s"randColumn${randomUUID().toString.replace("-", "")}"
        s"CREATE $materializedStr RESULT TABLE $tableName PARTITION BY ($randColName) AS SELECT *, RAND() AS $randColName FROM ($query)"
      } else {
        s"CREATE $materializedStr RESULT TABLE $tableName PARTITION BY (${repartitionColumns
          .mkString(",")}) AS SELECT * FROM ($query)"
      }
    } else {
      s"CREATE $materializedStr RESULT TABLE $tableName AS $query"
    }
  }

  def getSelectFromResultTableQuery(tableName: String, partition: Int): String = {
    s"SELECT * FROM ::$tableName WHERE partition_id() = $partition"
  }

  def createResultTable(conn: Connection,
                        tableName: String,
                        query: String,
                        schema: StructType,
                        variables: VariableList,
                        materialized: Boolean,
                        needsRepartition: Boolean,
                        repartitionColumns: Seq[String]): Unit = {
    val sql =
      getCreateResultTableQuery(tableName,
                                query,
                                schema,
                                materialized,
                                needsRepartition,
                                repartitionColumns)

    conn.withPreparedStatement(sql, stmt => {
      JdbcHelpers.fillStatement(stmt, variables)
      stmt.executeUpdate()
    })
  }

  def dropResultTable(conn: Connection, tableName: String): Unit = {
    val sql = s"DROP RESULT TABLE $tableName"
    log.info(logEventNameTagger(s"Executing SQL:\n$sql"))

    conn.withStatement(stmt => {
      stmt.executeUpdate(sql)
    })
  }

  def isValidQuery(conn: Connection, query: String, variables: VariableList): Boolean = {
    val sql = s"EXPLAIN $query"
    log.info(logEventNameTagger(s"Executing SQL:\n$sql"))

    Try {
      conn.withPreparedStatement(sql, stmt => {
        JdbcHelpers.fillStatement(stmt, variables)
        stmt.execute()
      })
    } match {
      case Success(_) => true
      // SQLInvalidAuthorizationSpecException is thrown when password (or token in the case of JWT authentification) is wrong
      case Failure(e: SQLException)
          if !e.isInstanceOf[SQLInvalidAuthorizationSpecException] &&
            // Original exception can be wrapped inside of the connection pool
            !(e.getCause != null && e.getCause
              .isInstanceOf[SQLInvalidAuthorizationSpecException]) =>
        false
    }
  }

  def truncateTable(conf: SinglestoreOptions, conn: Connection, table: TableIdentifier): Unit = {
    val sql = prepareAndLogSql(conf, s"TRUNCATE TABLE ${table.quotedString}")
    conn.withStatement(stmt => stmt.executeUpdate(sql))
  }

  def dropTable(conf: SinglestoreOptions, conn: Connection, table: TableIdentifier): Unit = {
    val sql = prepareAndLogSql(conf, s"DROP TABLE ${table.quotedString}")
    conn.withStatement(stmt => stmt.executeUpdate(sql))
  }

  def isReferenceTable(conf: SinglestoreOptions, table: TableIdentifier): Boolean = {
    val conn =
      SinglestoreConnectionPool.getConnection(getDDLConnProperties(conf, isOnExecutor = false))
    // Assume that either table.database is set or conf.database is set
    val databaseName =
      table.database
        .orElse(conf.database)
        .getOrElse(throw new IllegalArgumentException("Database name should be defined"))
    val sql = prepareAndLogSql(conf, s"using $databaseName show tables extended like '${table.table}'")
    val resultSet = conn.withStatement(stmt => {
      Try {
        try {
          stmt.executeQuery(sql)
        } finally {
          stmt.close()
          conn.close()
        }
      }
    })
    resultSet.toOption.fold(false)(resultSet => {
      if (resultSet.next()) {
        !resultSet.getBoolean("distributed")
      } else {
        throw new IllegalArgumentException(s"Table `$databaseName.${table.table}` doesn't exist")
      }
    })
  }

  def prepareTableForWrite(conf: SinglestoreOptions,
                           table: TableIdentifier,
                           mode: SaveMode,
                           schema: StructType): Unit = {
    val version = SinglestoreVersion(JdbcHelpers.getSinglestoreVersion(conf))
    val conn =
      SinglestoreConnectionPool.getConnection(getDDLConnProperties(conf, isOnExecutor = false))
    try {
      if (JdbcHelpers.tableExists(conf, conn, table)) {
        mode match {
          case SaveMode.Overwrite =>
            conf.overwriteBehavior match {
              case Truncate =>
                JdbcHelpers.truncateTable(conf, conn, table)
              case DropAndCreate =>
                JdbcHelpers.dropTable(conf, conn, table)
                JdbcHelpers.createTable(conf,
                                        conn,
                                        table,
                                        schema,
                                        conf.tableKeys,
                                        conf.createRowstoreTable,
                                        version)
              case Merge =>
              // nothing to do
            }
          case SaveMode.ErrorIfExists =>
            sys.error(
              s"Table '$table' already exists. SaveMode: ErrorIfExists."
            )
          case SaveMode.Ignore =>
          // table already exists, nothing to do
          case SaveMode.Append => // continue
        }
      } else {
        JdbcHelpers.createTable(conf,
                                conn,
                                table,
                                schema,
                                conf.tableKeys,
                                conf.createRowstoreTable,
                                version)
      }
    } finally {
      conn.close()
    }
  }

  private final def prepareAndLogSql(conf: SinglestoreOptions, query: String): String = {
    val finalQuery = appendTagsToQuery(conf, query)
    log.info(logEventNameTagger(s"Executing SQL:\n$finalQuery"))
    finalQuery
  }

  def appendTagsToQuery(conf: SinglestoreOptions, query: String): String = {
    val jdbcProps = conf.jdbcExtraOptions
    val aiqPropsString = jdbcProps.collect {
      case (k, v) if k.contains("aiq_") =>
        val regex = ".*aiq_".r
        regex.replaceAllIn(k, "") + ":" + v
    }.toSeq.sorted.mkString(",")

    val finalQuery = if (aiqPropsString.nonEmpty) {
      s"/* $aiqPropsString */\n$query"
    } else {
      query
    }

    finalQuery
  }
}
