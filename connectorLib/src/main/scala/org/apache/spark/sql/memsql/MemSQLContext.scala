package org.apache.spark.sql.memsql

import com.memsql.spark.connector.sql.TableIdentifier
import com.memsql.spark.connector.{MemSQLCluster, MemSQLConf}
import com.memsql.spark.pushdown.MemSQLPushdownStrategy
import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, Catalog}

class MemSQLContext(sparkContext: SparkContext) extends SQLContext(sparkContext) {

  // Mixin our pushdown strategy
  MemSQLPushdownStrategy.patchSQLContext(this)

  var memSQLConf: MemSQLConf = MemSQLConf(sparkContext.getConf)

  def setDatabase(dbName: String): Unit = memSQLConf = memSQLConf.copy(defaultDBName = dbName)
  def getDatabase: String = memSQLConf.defaultDBName

  def getMemSQLCluster: MemSQLCluster = MemSQLCluster(memSQLConf)

  def table(tableIdent: TableIdentifier): DataFrame =
    DataFrame(this, catalog.lookupRelation(tableIdent.toSeq))

  def maybeTable(tableName: String): Option[DataFrame] = {
    try {
      Some(super.table(tableName))
    } catch {
      case e: NoSuchTableException => None
    }
  }

  @transient
  override protected[sql] lazy val catalog: Catalog = new MemSQLCatalog(this, this.conf)

  override def sql(sqlText: String): DataFrame = {
    sql(sqlText, false)
  }

  def sql(sqlText: String, bypassCatalyst: Boolean): DataFrame = {
    if (bypassCatalyst) {
      val memSQLRelation = MemSQLQueryRelation(getMemSQLCluster, Some(getDatabase), sqlText, this)
      val logicalPlan = LogicalRelation(memSQLRelation)
      DataFrame(this, logicalPlan)
    } else {
      super.sql(sqlText)
    }
  }
}
