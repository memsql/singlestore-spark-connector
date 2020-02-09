package com.memsql.spark

import java.sql.{Connection, PreparedStatement, Statement}
import java.util.Optional

import com.memsql.spark.SQLGen.VariableList
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType

import scala.util.Try

object JdbcHelpers extends LazyLogging {
  final val MEMSQL_CONNECT_TIMEOUT = "10" // seconds

  // register the MemsqlDialect
  JdbcDialects.registerDialect(MemsqlDialect)

  // Connection implicits
  implicit class ConnectionHelpers(val conn: Connection) {
    def withStatement[T](handle: Statement => T): T =
      Loan(conn.createStatement).to(handle)

    def withPreparedStatement[T](query: String, handle: PreparedStatement => T): T =
      Loan(conn.prepareStatement(query)).to(handle)
  }

  def getJDBCOptions(conf: MemsqlOptions, hostports: String*): JDBCOptions = {
    val url: String = {
      val base = s"jdbc:mysql://${hostports.mkString(",")}"
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
    ).mkString(";")

    new JDBCOptions(
      Map(
        JDBCOptions.JDBC_URL        -> url,
        JDBCOptions.JDBC_TABLE_NAME -> "XXX",
        "user"                      -> conf.user,
        "password"                  -> conf.password,
        "zeroDateTimeBehavior"      -> "convertToNull",
        "allowLoadLocalInfile"      -> "true",
        "connectTimeout"            -> MEMSQL_CONNECT_TIMEOUT,
        "sessionVariables"          -> sessionVariables
      ) ++ conf.jdbcExtraOptions
    )
  }

  def getDDLJDBCOptions(conf: MemsqlOptions): JDBCOptions =
    getJDBCOptions(conf, conf.ddlEndpoint)

  def getDMLJDBCOptions(conf: MemsqlOptions): JDBCOptions =
    getJDBCOptions(conf, conf.dmlEndpoints: _*)

  def loadSchema(conf: MemsqlOptions, query: String, variables: VariableList): StructType = {
    val conn = JdbcUtils.createConnectionFactory(getDDLJDBCOptions(conf))()
    try {
      val statement =
        conn.prepareStatement(MemsqlDialect.getSchemaQuery(s"($query) AS q"))
      try {
        fillStatement(statement, variables)
        val rs = statement.executeQuery()
        try {
          JdbcUtils.getSchema(rs, MemsqlDialect, alwaysNullable = true)
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

  def explainQuery(conf: MemsqlOptions, query: String, variables: VariableList): String = {
    val conn = JdbcUtils.createConnectionFactory(getDDLJDBCOptions(conf))()
    try {
      val statement = conn.prepareStatement(s"EXPLAIN ${query}")
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

  def schemaToString(schema: StructType): String = {
    schema.fields
      .map(field => {
        val name = MemsqlDialect.quoteIdentifier(field.name)
        val typ = MemsqlDialect
          .getJDBCType(field.dataType)
          .getOrElse(
            throw new IllegalArgumentException(
              s"Can't get JDBC type for ${field.dataType.simpleString}"
            )
          )
        val nullable = if (field.nullable) "" else " NOT NULL"
        s"${name} ${typ.databaseTypeDefinition}${nullable}"
      })
      .mkString("(\n  ", ",\n  ", "\n)")
  }

  def tableExists(conn: Connection, table: TableIdentifier): Boolean = {
    conn.withStatement(
      stmt =>
        Try {
          try {
            stmt.execute(MemsqlDialect.getTableExistsQuery(table.quotedString))
          } finally {
            stmt.close()
          }
        }.isSuccess
    )
  }

  def createTable(conn: Connection, table: TableIdentifier, schema: StructType): Unit = {
    val sql = s"CREATE TABLE ${table.quotedString} ${schemaToString(schema)}"
    log.trace(s"Executing SQL:\n$sql")
    conn.withStatement(stmt => stmt.executeUpdate(sql))
  }

  def truncateTable(conn: Connection, table: TableIdentifier): Unit = {
    val sql = s"TRUNCATE TABLE ${table.quotedString}"
    log.trace(s"Executing SQL:\n$sql")
    conn.withStatement(stmt => stmt.executeUpdate(sql))
  }

  def dropTable(conn: Connection, table: TableIdentifier): Unit = {
    val sql = s"DROP TABLE ${table.quotedString}"
    log.trace(s"Executing SQL:\n$sql")
    conn.withStatement(stmt => stmt.executeUpdate(sql))
  }

  def prepareTableForWrite(conf: MemsqlOptions,
                           table: TableIdentifier,
                           mode: SaveMode,
                           schema: StructType): Unit = {
    val jdbcOpts = JdbcHelpers.getDDLJDBCOptions(conf)
    val conn     = JdbcUtils.createConnectionFactory(jdbcOpts)()
    try {
      if (JdbcHelpers.tableExists(conn, table)) {
        mode match {
          case SaveMode.Overwrite =>
            if (conf.truncate) {
              JdbcHelpers.truncateTable(conn, table)
            } else {
              JdbcHelpers.dropTable(conn, table)
              JdbcHelpers.createTable(conn, table, schema)
            }
          case SaveMode.ErrorIfExists =>
            sys.error(
              s"Table '${table}' already exists. SaveMode: ErrorIfExists."
            )
          case SaveMode.Ignore =>
          // table already exists, nothing to do
          case SaveMode.Append => // continue
        }
      } else {
        JdbcHelpers.createTable(conn, table, schema)
      }
    } finally {
      conn.close()
    }
  }
}
