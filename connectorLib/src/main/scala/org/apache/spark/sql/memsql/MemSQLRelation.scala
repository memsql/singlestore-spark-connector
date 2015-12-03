package org.apache.spark.sql.memsql


import com.memsql.spark.connector.MemSQLCluster
import com.memsql.spark.connector.rdd.MemSQLRDD
import com.memsql.spark.connector.sql.TableIdentifier
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{InsertableRelation, BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SQLContext}

import com.memsql.spark.connector.util.JDBCImplicits._
import SparkImplicits._

case class MemSQLRelation(cluster: MemSQLCluster,
                          tableIdentifier: TableIdentifier,
                          sqlContext: SQLContext) extends BaseRelation
                                                  with TableScan
                                                  with InsertableRelation {

  val database: Option[String] = tableIdentifier.database
  val query: String = s"SELECT * FROM ${tableIdentifier.quotedString}"

  lazy val schema: StructType = cluster.getQuerySchema(query)
  lazy val output: Seq[Attribute] = schema.toAttributes

  override def buildScan: RDD[Row] = {
    MemSQLRDD(sqlContext.sparkContext, cluster, query,
              databaseName=tableIdentifier.database,
              mapRow=_.toRow)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val mode = if (overwrite) { SaveMode.Overwrite } else { SaveMode.Append }
    insert(data, SaveToMemSQLConf(cluster.conf, Some(mode)))
  }

  def insert(data: DataFrame, saveConf: SaveToMemSQLConf): Unit =
    data.saveToMemSQL(tableIdentifier, saveConf)
}

object MemSQLRelationUtils {
  def unapply(l: LogicalRelation): Option[MemSQLRelation] = l match {
    case LogicalRelation(r: TableScan) => r match {
      case r: MemSQLRelation => Some(r)
      case _ => None
    }
  }
}
