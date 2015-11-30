package org.apache.spark.sql.memsql

import java.sql.ResultSet

import com.memsql.spark.connector.MemSQLCluster
import com.memsql.spark.connector.dataframe.MemSQLDataFrameUtils
import com.memsql.spark.connector.rdd.MemSQLRDD
import org.apache.commons.lang.NotImplementedException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{InsertableRelation, BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

case class MemSQLRelation(cluster: MemSQLCluster,
                          tableIdentifier: TableIdentifier,
                          schema: StructType,
                          sqlContext: SQLContext) extends BaseRelation
                                                  with TableScan with InsertableRelation {

  val database: Option[String] = tableIdentifier.database
  val query: String = s"SELECT * FROM ${tableIdentifier.quotedString}"

  val output: Seq[Attribute] = schema.toAttributes

  override def buildScan: RDD[Row] = {
    MemSQLRDD(sqlContext.sparkContext, cluster, query,
              databaseName=tableIdentifier.database,
              mapRow=MemSQLRelationUtils.resultSetToRow)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    // TODO: Save data into the table defined by tableIdentifier
    throw new NotImplementedException("MemSQLRelation doesn't support inserting DataFrames quite yet.")
  }
}

object MemSQLRelationUtils {
  def resultSetToRow(result: ResultSet): Row = {
    val columnCount = result.getMetaData.getColumnCount

    Row.fromSeq(Range(0, columnCount).map(i => {
      val columnType = result.getMetaData.getColumnType(i + 1)
      MemSQLDataFrameUtils.GetJDBCValue(columnType, i + 1, result)
    }))
  }

  def unapply(l: LogicalRelation): Option[MemSQLRelation] = l match {
    case LogicalRelation(r: TableScan) => r match {
      case r: MemSQLRelation => Some(r)
      case _ => None
    }
  }
}
