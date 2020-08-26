package com.memsql.spark.v2

import java.util

import com.memsql.spark.SQLGen.SQLGenContext
import com.memsql.spark.{DefaultSource, JdbcHelpers, MemsqlOptions}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MemsqlTableCatalog extends TableCatalog with SupportsNamespaces {

  override def listNamespaces(): Array[Array[String]] = {
    val sparkSession = SparkSession.active
    val opts         = CaseInsensitiveMap(includeGlobalParams(sparkSession.sparkContext, Map.empty))
    val conf         = MemsqlOptions(opts)
    Array(JdbcHelpers.showDatabases(conf))
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    listNamespaces()
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    val sparkSession = SparkSession.active
    val opts         = CaseInsensitiveMap(includeGlobalParams(sparkSession.sparkContext, Map.empty))
    val conf         = MemsqlOptions(opts)
    val databases    = JdbcHelpers.showDatabases(conf)
    for (name <- namespace) {
      if (!databases.contains(name)) throw new NoSuchNamespaceException(namespace)
    }
    new util.HashMap[String, String]()
  }

  override def createNamespace(namespace: Array[String],
                               metadata: util.Map[String, String]): Unit = {
    val sparkSession = SparkSession.active
    val opts         = CaseInsensitiveMap(includeGlobalParams(sparkSession.sparkContext, Map.empty))
    val conf         = MemsqlOptions(opts)
    for (name <- namespace) {
      JdbcHelpers.createDatabase(conf, name)
    }
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    println("alterNamespace")
  }

  override def dropNamespace(namespace: Array[String]): Boolean = {
    val sparkSession = SparkSession.active
    val opts         = CaseInsensitiveMap(includeGlobalParams(sparkSession.sparkContext, Map.empty))
    val conf         = MemsqlOptions(opts)
    for (name <- namespace) {
      if (!JdbcHelpers.dropDatabase(conf, name)) false
    }
    true
  }

  private def getQuery(t: TableIdentifier): String = {
    s"SELECT * FROM ${t.quotedString}"
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    val sparkSession = SparkSession.active
    val opts         = CaseInsensitiveMap(includeGlobalParams(sparkSession.sparkContext, Map.empty))
    val conf         = MemsqlOptions(opts)
    var identifiers  = Array.empty[Identifier]
    for (database <- namespace) {
      identifiers = identifiers ++ JdbcHelpers
        .showTables(database, conf)
        .map(table => MemsqlIdentifier(database, table))
    }
    identifiers
  }

  override def loadTable(ident: Identifier): Table = {
    val sparkSession    = SparkSession.active
    val opts            = CaseInsensitiveMap(includeGlobalParams(sparkSession.sparkContext, Map.empty))
    val conf            = MemsqlOptions(opts)
    val jdbcOpts        = JdbcHelpers.getDDLJDBCOptions(conf)
    val conn            = JdbcUtils.createConnectionFactory(jdbcOpts)()
    val tableIdentifier = TableIdentifier(ident.name(), Some(ident.namespace()(0)))
    if (!JdbcHelpers.tableExists(conn, tableIdentifier)) {
      throw new NoSuchTableException("table doesn't exist")
    }
    MemsqlTable(getQuery(tableIdentifier),
                Nil,
                conf,
                sparkSession.sparkContext,
                tableIdentifier,
                context = SQLGenContext(conf))
  }

  private def includeGlobalParams(sqlContext: SparkContext,
                                  params: Map[String, String]): Map[String, String] =
    sqlContext.getConf.getAll.foldLeft(params)({
      case (params, (k, v)) if k.startsWith(DefaultSource.MEMSQL_GLOBAL_OPTION_PREFIX) =>
        params + (k.stripPrefix(DefaultSource.MEMSQL_GLOBAL_OPTION_PREFIX) -> v)
      case (params, _) => params
    })

  override def createTable(ident: Identifier,
                           schema: StructType,
                           partitions: Array[Transform],
                           properties: util.Map[String, String]): Table = {
    val sparkSession    = SparkSession.active
    val opts            = CaseInsensitiveMap(includeGlobalParams(sparkSession.sparkContext, Map.empty))
    val conf            = MemsqlOptions(opts)
    val jdbcOpts        = JdbcHelpers.getDDLJDBCOptions(conf)
    val conn            = JdbcUtils.createConnectionFactory(jdbcOpts)()
    val tableIdentifier = TableIdentifier(ident.name(), Some(ident.namespace()(0)))
    if (!JdbcHelpers.tableExists(conn, tableIdentifier)) {
      JdbcHelpers.createTable(conn, tableIdentifier, schema, List.empty)
    }
    MemsqlTable(getQuery(tableIdentifier),
                Nil,
                conf,
                sparkSession.sparkContext,
                tableIdentifier,
                context = SQLGenContext(conf))
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = ???

  override def dropTable(ident: Identifier): Boolean = ???

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = ???

  override def name(): String = "memsql"

}
