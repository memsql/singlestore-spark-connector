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
      val userTableIdent = MemSQLCatalog.makeTableIdentifier(tableIdentifier)
      val actualTableIdent = lookupTable(userTableIdent)

      if (actualTableIdent.isEmpty) {
        throw new AnalysisException(
          s"MemSQLCatalog failed to find table with name ${userTableIdent.quotedString}"
        )
      }

      val relation = LogicalRelation(MemSQLRelation(
        cluster=cluster,
        tableIdentifier=actualTableIdent.get,
        sqlContext=msc
      ))

      alias.map(a => Subquery(a, relation)).getOrElse(relation)
    }
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val simpleTables = super.getTables(databaseName)
    val result = getDBTablePairs(databaseName).map(t => {
      (t.quotedString, true)
    })
    simpleTables.union(result)
  }

  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val tableId = MemSQLCatalog.makeTableIdentifier(tableIdentifier)
    lookupTable(tableId).isDefined || super.tableExists(tableIdentifier)
  }

  def lookupTable(tableId: TableIdentifier): Option[TableIdentifier] = {
    getDBTablePairs(tableId.database).find(tableIdent =>
      tableIdent.table == tableId.table
    )
  }

  def getDBTablePairs(databaseName: Option[String]): Seq[TableIdentifier] = {
    msc.getMemSQLCluster.withAggregatorConn { conn =>
      MemSQLCatalog.queryTables(conn, databaseName, msc)(r => {
        val (dbName, tableName) = (r.getString("table_schema"), r.getString("table_name"))
        TableIdentifier(dbName, tableName)
      })
    }
  }
}

object MemSQLCatalog {
  def queryTables[T](conn: Connection, databaseName: Option[String], msc: MemSQLContext)(handler: ResultSet => T): List[T] = {
    val sql = "SELECT table_schema, table_name FROM information_schema.tables"
    val dbName = databaseName.getOrElse(msc.getDatabase)

    conn.withPreparedStatement(sql + " WHERE table_schema = ?", stmt => {
      stmt.setString(1, dbName)
      stmt.executeQuery.toIterator.map(handler).toList
    })
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
