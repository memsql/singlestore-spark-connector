package org.apache.spark.sql.memsql


import com.memsql.spark.connector.MemSQLCluster
import com.memsql.spark.connector.rdd.MemSQLRDD
import com.memsql.spark.connector.sql.TableIdentifier
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{InsertableRelation, BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SQLContext}

import com.memsql.spark.connector.util.JDBCImplicits._
import SparkImplicits._

abstract class MemSQLRelation extends BaseRelation with TableScan {

  def cluster: MemSQLCluster
  def database: Option[String]
  def query: String
  def sqlContext: SQLContext

  lazy val schema: StructType = cluster.getQuerySchema(query)

  override def buildScan: RDD[Row] = {
    MemSQLRDD(sqlContext.sparkContext, cluster, query,
              databaseName=database,
              mapRow=_.toRow)
  }
}

case class MemSQLTableRelation(cluster: MemSQLCluster,
                               tableIdentifier: TableIdentifier,
                               sqlContext: SQLContext) extends MemSQLRelation with InsertableRelation {

  override def database: Option[String] = tableIdentifier.database
  override def query: String = s"SELECT * FROM ${tableIdentifier.quotedString}"

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val mode = if (overwrite) { SaveMode.Overwrite } else { SaveMode.Append }
    insert(data, SaveToMemSQLConf(cluster.conf, Some(mode)))
  }

  def insert(data: DataFrame, saveConf: SaveToMemSQLConf): Unit =
    data.saveToMemSQL(tableIdentifier, saveConf)
}

case class MemSQLQueryRelation(cluster: MemSQLCluster,
                               database: Option[String],
                               query: String,
                               sqlContext: SQLContext) extends MemSQLRelation

object UnpackLogicalRelation {
  def unapply(l: LogicalRelation): Option[MemSQLRelation] = l match {
    case LogicalRelation(r: TableScan, _) => r match {
      case r: MemSQLRelation => Some(r)
      case _ => None
    }
  }
}
