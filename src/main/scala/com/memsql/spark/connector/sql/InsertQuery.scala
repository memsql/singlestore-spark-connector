package com.memsql.spark.connector.sql

import java.sql.Connection

import org.apache.spark.sql.{SaveMode, Row}

import com.memsql.spark.connector.util.JDBCImplicits._

class InsertQuery(tableFragment: QueryFragment,
                  mode: SaveMode,
                  onDupKeySQL: Option[String]=None) {

  val insertType = onDupKeySQL match {
    case Some(_) => "INSERT"
    case None => mode match {
      case SaveMode.Append => "INSERT IGNORE"
      case SaveMode.Ignore => "INSERT IGNORE"
      case SaveMode.Overwrite => "REPLACE"
      case SaveMode.ErrorIfExists => "INSERT"
    }
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

  def makeValueTemplate(rowLength: Int): QueryFragment =
    QueryFragment().block(_.raw(("?" * rowLength).mkString(",")))

  def buildQuery(rows: List[Row]): String = {
    if (rows.isEmpty) {
      throw new IllegalArgumentException("`rows` must not be empty.")
    }

    val rowLength = rows(0).length
    rows.foreach(r => {
      if (r.length != rowLength) {
        throw new IllegalArgumentException("`rows` must contain Row objects of the same length.")
      }
    })

    val valueTemplate = makeValueTemplate(rowLength)
    QueryFragment()
      .fragment(insertPrefix)
      .addFragments(Stream.continually(valueTemplate).take(rows.length), ",")
      .fragment(insertSuffix)
      .sql.toString
  }

  def flush(conn: Connection, rows: List[Row]): Int = {
    val query = buildQuery(rows)
    conn.withPreparedStatement(query, stmt => {
      stmt.fillParams(rows.flatMap(_.toSeq))
      stmt.executeUpdate()
    })
  }
}
