package com.memsql.spark.v2

import java.util

import com.memsql.spark.SQLGen.SQLGenContext
import com.memsql.spark.{DefaultSource, JdbcHelpers, MemsqlOptions}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MemsqlTableCatalog extends StagingTableCatalog with SupportsNamespaces {

  override def listNamespaces(): Array[Array[String]] = Array.empty

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = Array.empty

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] =
    throw new NoSuchNamespaceException(namespace)

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit =
    new util.HashMap[String, String]()

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {}

  override def dropNamespace(namespace: Array[String]): Boolean = true

  override def stageCreate(ident: Identifier,
                           schema: StructType,
                           partitions: Array[Transform],
                           properties: util.Map[String, String]): StagedTable = {
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

  private def getQuery(t: TableIdentifier): String = {
    s"SELECT * FROM ${t.quotedString}"
  }

  override def stageReplace(ident: Identifier,
                            schema: StructType,
                            partitions: Array[Transform],
                            properties: util.Map[String, String]): StagedTable = ???

  override def stageCreateOrReplace(ident: Identifier,
                                    schema: StructType,
                                    partitions: Array[Transform],
                                    properties: util.Map[String, String]): StagedTable = ???

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}

  override def listTables(namespace: Array[String]): Array[Identifier] = ???

  override def loadTable(ident: Identifier): Table = {
    val sparkSession    = SparkSession.active
    val opts            = CaseInsensitiveMap(includeGlobalParams(sparkSession.sparkContext, Map.empty))
    val conf            = MemsqlOptions(opts)
    val jdbcOpts        = JdbcHelpers.getDDLJDBCOptions(conf)
    val conn            = JdbcUtils.createConnectionFactory(jdbcOpts)()
    val tableIdentifier = TableIdentifier(ident.name(), Some(ident.namespace()(0)))
    if (JdbcHelpers.tableExists(conn, tableIdentifier)) {
      MemsqlTable(getQuery(tableIdentifier),
                  Nil,
                  conf,
                  sparkSession.sparkContext,
                  tableIdentifier,
                  context = SQLGenContext(conf))
    } else null
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
