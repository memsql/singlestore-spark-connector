package com.memsql.spark.connector

import org.apache.commons.lang3.StringUtils
import java.sql.{Connection, Date, PreparedStatement, ResultSet, SQLException, Timestamp}

import com.memsql.spark.connector.rdd.MemSQLRDD
import com.memsql.spark.connector.sql.TableIdentifier
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import com.memsql.spark.connector.util.JDBCImplicits._

case class MemSQLQueryRelation(cluster: MemSQLCluster,
                               query: String,
                               sqlContext: SQLContext) extends BaseRelation with TableScan {

  override def schema: StructType = cluster.getQuerySchema(query)

  def buildScan(): RDD[Row] = {
    MemSQLRDD(
      sqlContext.sparkContext,
      cluster,
      query,
      mapRow=_.toRow
    )
  }
}

case class MemSQLTableRelation(cluster: MemSQLCluster,
                               tableIdentifier: TableIdentifier,
                               sqlContext: SQLContext)
  extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation {

  val database: Option[String] = tableIdentifier.database

  override def schema: StructType = cluster.getQuerySchema(s"SELECT * FROM ${tableIdentifier.quotedString}")

  // TableScan
  def buildScan(): RDD[Row] = {
    val queryString = s"SELECT * FROM ${tableIdentifier.quotedString}"
    MemSQLRDD(sqlContext.sparkContext,
      cluster,
      queryString,
      databaseName=database,
      mapRow=_.toRow)
  }

  //PrunedScan
  def buildScan(requiredColumns: Array[String]): RDD[Row] = buildScan(requiredColumns, Array.empty)

  //PrunedFilteredScan
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val columnListAsString: String = {
      val sb = new StringBuilder()
      requiredColumns.foreach(x => sb.append(",").append(x))
      if (sb.isEmpty) "1" else sb.substring(1)
    }

    val whereClauseAsString: String = {
      filters
        .flatMap(compileFilter(_))
        .map(p => s"($p)").mkString(" AND ")
    }

    val finalWhereString: String = {
      if (whereClauseAsString.length > 0) {
        "WHERE " + whereClauseAsString
      } else {
        ""
      }
    }

    val queryString: String = s"SELECT ${columnListAsString} FROM ${tableIdentifier.quotedString} $finalWhereString"

    MemSQLRDD(sqlContext.sparkContext,
      cluster,
      queryString,
      databaseName=database,
      mapRow=_.toRow)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter(compileFilter(_).isEmpty)
  }

  def compileFilter(f: Filter): Option[String] = {
   Option(f match {
     case EqualTo(attr, value) => s"""$attr = '${extract(value)}'"""
     case LessThan(attr, value) => s"""$attr < '${extract(value)}'"""
     case GreaterThan(attr, value) => s"""$attr > '${extract(value)}'"""
     case LessThanOrEqual(attr, value) => s"""$attr <= '${extract(value)}'"""
     case GreaterThanOrEqual(attr, value) => s"""$attr >= '${extract(value)}'"""
     case IsNotNull(attr) => s"""$attr IS NOT NULL"""
     case IsNull(attr) => s"""$attr IS NULL"""
     case StringStartsWith(attr, value) => s"""$attr LIKE '${extract(value)}%'"""
     case StringEndsWith(attr, value) => s"""$attr LIKE '%${extract(value)}'"""
     case StringContains(attr, value) => s"""$attr LIKE '%${extract(value)}%'"""
     case In(attr, value) if value.isEmpty =>
       s"""CASE WHEN $attr IS NULL THEN NULL ELSE FALSE END"""
     case In(attr, value) => s"""$attr IN (${extract(value)})"""
     case Not(f) => compileFilter(f).map(p => s"(NOT ($p))").getOrElse(null)
     case Or(f1, f2) =>
       val or = Seq(f1, f2).flatMap(compileFilter(_))
       if (or.size == 2) {
         or.map(p => s"($p)").mkString(" OR ")
       } else {
         null
       }
     case And(f1, f2) =>
       val and = Seq(f1, f2).flatMap(compileFilter(_))
       if (and.size == 2) {
         and.map(p => s"($p)").mkString(" AND ")
       } else {
         null
       }
     case _ => null
   })
  }

  def extract(value: Any): String = {
    value match {
      case _: Boolean |
        _: Byte       |
        _: Short      |
        _: Int        |
        _: Long       |
        _: Float      |
        _: Double     |
        _: BigDecimal   => value.toString
      case stringValue: String => stringValue
      case timestampValue: Timestamp => timestampValue.toString
      case dateValue: Date => dateValue.toString
      case arrayValue: Array[Any] => arrayValue.map(extract).mkString(", ")
      case _ => value.toString
    }
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val mode = if (overwrite) { SaveMode.Overwrite } else { SaveMode.Append }
    insert(data, SaveToMemSQLConf(cluster.conf, Some(mode)))
  }

  def insert(data: DataFrame, saveConf: SaveToMemSQLConf): Unit =
    data.saveToMemSQL(tableIdentifier, saveConf)
}
