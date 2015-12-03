package org.apache.spark.sql.memsql

import com.memsql.spark.connector.{MemSQLCluster, MemSQLConf}
import com.memsql.spark.pushdown.MemSQLPushdownStrategy
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.{CatalystConf, SimpleCatalystConf}
import org.apache.spark.sql.{SQLConf, SQLContext}
import org.apache.spark.sql.catalyst.analysis.Catalog

class MemSQLContext(sparkContext: SparkContext) extends SQLContext(sparkContext) {

  // Mixin our pushdown strategy
  MemSQLPushdownStrategy.patchSQLContext(this)

  var memSQLConf: MemSQLConf = MemSQLConf(sparkContext.getConf)

  def setDatabase(dbName: String): Unit = memSQLConf = memSQLConf.copy(defaultDBName = dbName)
  def getDatabase: String = memSQLConf.defaultDBName

  def getMemSQLCluster: MemSQLCluster = MemSQLCluster(memSQLConf)

  @transient
  override protected[sql] lazy val catalog: Catalog = new MemSQLCatalog(this, this.conf)
}
