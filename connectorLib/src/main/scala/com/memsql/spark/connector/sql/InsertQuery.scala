package com.memsql.spark.connector.sql

import java.sql.{Connection, Statement}

import org.apache.spark.sql.{SaveMode, Row}

import scala.collection.mutable.ListBuffer

import com.memsql.spark.connector.util.JDBCImplicits._

class InsertQuery(tableFragment: QueryFragment,
                  mode: SaveMode,
                  onDupKeySQL: Option[String]=None) {

  val insertType = mode match {
    case SaveMode.Append => "INSERT IGNORE"
    case SaveMode.Ignore => "INSERT IGNORE"
    case SaveMode.Overwrite => "REPLACE"
    case SaveMode.ErrorIfExists => "INSERT"
  }

  val insertPrefix =
    QueryFragment()
      .raw(insertType)
      .raw(" INTO ")
      .fragment(tableFragment)
      .raw(" VALUES ")

  val insertSuffix = onDupKeySQL.map(dks => {
    QueryFragment()
      .raw(" ON DUPLICATE KEY UPDATE ")
      .raw(dks)
  })

  var rowLength: Int = 0
  lazy val rowTemplate =
    QueryFragment().block(_.raw(("?" * rowLength).mkString(",")))

  val queryBuffer = QueryFragment()
  val rowBuffer: ListBuffer[Row] = ListBuffer.empty

  def addRow(row: Row): InsertQuery = {
    rowLength = row.length
    queryBuffer.fragment(rowTemplate)
    rowBuffer :+ row
    this
  }

  def flush(conn: Connection): Int = {
    val query = QueryFragment()
      .fragment(insertPrefix)
      .fragment(queryBuffer)
      .fragment(insertSuffix)
      .sql.toString

    val numRowsAffected = conn.withPreparedStatement(query, stmt => {
      for (row <- rowBuffer) {
        stmt.fillParams(row.toSeq)
      }
      stmt.executeUpdate()
    })

    queryBuffer.clear
    rowBuffer.clear
    numRowsAffected
  }
}
