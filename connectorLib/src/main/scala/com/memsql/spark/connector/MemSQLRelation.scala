package com.memsql.spark.connector

import java.sql.{Date, Timestamp}

import com.memsql.spark.connector.rdd.MemSQLRDD
import com.memsql.spark.connector.sql.TableIdentifier
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import com.memsql.spark.connector.util.JDBCImplicits._

import scala.collection.mutable.ListBuffer

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

    val params: ListBuffer[Any] = new ListBuffer[Any]()

    val whereClauseAsString: String = {
      filters
        .flatMap(compileFilter(_, params))
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
      sqlParams=params,
      databaseName=database,
      mapRow=_.toRow)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    def isHandled(f: Filter): Boolean = {
      f match {
        case EqualTo(_, _)          |
          LessThan(_, _)            |
          GreaterThan(_, _)         |
          LessThanOrEqual(_, _)     |
          GreaterThanOrEqual(_, _)  |
          IsNotNull(_)              |
          IsNull(_)                 |
          StringStartsWith(_, _)    |
          StringEndsWith(_, _)      |
          StringContains(_, _)      |
          In(_, _)                  |
          Not(_)                    |
          Or(_, _)                  |
          And(_, _)                => true
        case _ => false
      }
    }
    filters.filter(!isHandled(_))
  }

  def compileFilter(f: Filter, params: ListBuffer[Any]): Option[String] = {
    Option(f match {
      case EqualTo(attr, value) => {
        params += value
        s"""$attr = ?"""
      }
      case LessThan(attr, value) => {
        params += value
        s"""$attr < ?"""
      }
      case GreaterThan(attr, value) => {
        params += value
        s"""$attr > ?"""
      }
      case LessThanOrEqual(attr, value) => {
        params += value
        s"""$attr <= ?"""
      }
      case GreaterThanOrEqual(attr, value) => {
        params += value
        s"""$attr >= ?"""
      }
      case IsNotNull(attr) => s"""$attr IS NOT NULL"""
      case IsNull(attr) => s"""$attr IS NULL"""
      case StringStartsWith(attr, value) => {
        params += s"${escapeWildcards(value)}%"
        s"""$attr LIKE ?"""
      }
      case StringEndsWith(attr, value) => {
        params += s"%${escapeWildcards(value)}"
        s"""$attr LIKE ?"""
      }
      case StringContains(attr, value) => {
        params += s"%${escapeWildcards(value)}%"
        s"""$attr LIKE ?"""
      }
      case In(attr, value) if value.isEmpty =>
        s"""CASE WHEN $attr IS NULL THEN NULL ELSE FALSE END"""
      case In(attr, value) => {
        params ++= value
        s"""$attr IN (${(1 to value.length).map(_ => "?").mkString(", ")})"""
      }
      case Not(f) => compileFilter(f, params).map(p => s"(NOT ($p))").getOrElse(null)
      case Or(f1, f2) =>
        val or = Seq(f1, f2).flatMap(compileFilter(_, params))
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" OR ")
        } else {
          null
        }
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(compileFilter(_, params))
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" AND ")
        } else {
          null
        }
      case _ => null
    })
  }

  def escapeWildcards(s: String): String = {
    val percent = s.replaceAll("%", """\\%""")
    percent.replaceAll("_", """\\_""")
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val mode = if (overwrite) { SaveMode.Overwrite } else { SaveMode.Append }
    insert(data, SaveToMemSQLConf(cluster.conf, Some(mode)))
  }

  def insert(data: DataFrame, saveConf: SaveToMemSQLConf): Unit =
    data.saveToMemSQL(tableIdentifier, saveConf)
}
