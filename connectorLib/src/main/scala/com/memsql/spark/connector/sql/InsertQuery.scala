package com.memsql.spark.connector.sql

import java.sql.{Connection, Statement}

import org.apache.spark.sql.{SaveMode, Row}

import scala.collection.mutable.ListBuffer

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

  def flush(conn: Connection, rows: List[Row]): Int = {
    val valueTemplate = makeValueTemplate(rows(0).length)

    val query = QueryFragment()
      .fragment(insertPrefix)
      .addFragments(rows.map(_ => valueTemplate), ",")
      .fragment(insertSuffix)
      .sql.toString

    conn.withPreparedStatement(query, stmt => {
      stmt.fillParams(rows.map(_.toSeq).flatten)
      stmt.executeUpdate()
    })
  }
}
