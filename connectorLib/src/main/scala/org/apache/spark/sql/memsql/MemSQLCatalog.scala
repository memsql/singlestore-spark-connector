package org.apache.spark.sql.memsql

import java.sql.{ResultSet, Connection}

import com.memsql.spark.connector.sql.TableIdentifier
import org.apache.spark.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis.SimpleCatalog
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}

import com.memsql.spark.connector.util.JDBCImplicits._
import org.apache.spark.sql.execution.datasources.LogicalRelation

class MemSQLCatalog(val msc: MemSQLContext,
                    override val conf: CatalystConf
                   ) extends SimpleCatalog(conf) with Logging {

  override def lookupRelation(tableIdentifier: Seq[String], alias: Option[String]): LogicalPlan = {
    if (tableIdentifier.length == 1 && super.tableExists(tableIdentifier)) {
      super.lookupRelation(tableIdentifier, alias)
    } else {
      val cluster = msc.getMemSQLCluster
      val tableIdent = MemSQLCatalog.makeTableIdentifier(tableIdentifier)

      val relation = LogicalRelation(MemSQLRelation(
        cluster=cluster,
        tableIdentifier=tableIdent,
        sqlContext=msc
      ))

      alias.map(a => Subquery(a, relation)).getOrElse(relation)
    }
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val simpleTables = super.getTables(databaseName)

    val result = msc.getMemSQLCluster.withAggregatorConn { conn =>
      MemSQLCatalog.queryTables(conn, databaseName)(r => {
        val (dbName, tableName) = (r.getString("table_schema"), r.getString("table_name"))
        (s"`$dbName`.`$tableName`", true)
      })
    }

    simpleTables.union(result)
  }

  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val tableId = MemSQLCatalog.makeTableIdentifier(tableIdentifier)
    getTables(tableId.database).exists({
      case (tableName: String, _) =>
        tableName == processTableIdentifier(tableIdentifier).mkString(".")
    })
  }
}

object MemSQLCatalog {
  def queryTables[T](conn: Connection, databaseName: Option[String])(handler: ResultSet => T): List[T] = {
    val sql = "SELECT table_schema, table_name FROM information_schema.tables"

    if (databaseName.isDefined) {
      conn.withPreparedStatement(sql + " WHERE table_schema = ?", stmt => {
        stmt.setString(0, databaseName.get)
        stmt.executeQuery.toIterator.map(handler).toList
      })
    } else {
      conn.withStatement(stmt =>
        stmt.executeQuery(sql).toIterator.map(handler).toList)
    }
  }

  def makeTableIdentifier(tableIdentifier: Seq[String]): TableIdentifier = {
    if (tableIdentifier.length == 1) {
      TableIdentifier(tableIdentifier(0))
    } else if (tableIdentifier.length == 2) {
      TableIdentifier(tableIdentifier(1), Some(tableIdentifier(0)))
    } else {
      throw new AnalysisException(
        """
          |MemSQLContext table identifiers must be in the form
          |Seq(databaseName, tableName) or Seq(tableName)
        """.stripMargin
      )
    }
  }
}
