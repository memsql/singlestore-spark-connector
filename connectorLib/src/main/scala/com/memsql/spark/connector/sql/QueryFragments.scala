package com.memsql.spark.connector.sql

case class QueryFragment(sql: StringBuilder = new StringBuilder) {

  def clear: QueryFragment = { sql.clear; this }

  def space: QueryFragment = { sql.append(" "); this }

  def fragment(qf: QueryFragment): QueryFragment = raw(qf.sql)
  def fragment(qf: Option[QueryFragment]): QueryFragment = qf match {
    case Some(q) => fragment(q)
    case None => this
  }

  def raw(s: StringBuilder): QueryFragment = { sql.append(s); this }
  def raw(s: String): QueryFragment = { sql.append(s); this }

  def quoted(s: String): QueryFragment = { raw("`").raw(s).raw("`"); this }

  def block(inner: QueryFragment => Unit): QueryFragment = { raw("("); inner(this); raw(")") }

  def addFragments(qfs: Seq[QueryFragment], conjunction: String=" "): QueryFragment = {
    for (i <- qfs.indices) {
      if (i != 0) {
        raw(conjunction)
      }
      fragment(qfs(i))
    }
    this
  }
}

object QueryFragments {
  def tableNameWithColumns(tableIdentifier: TableIdentifier, columns: Seq[MemSQLColumn]): QueryFragment = {
    QueryFragment()
      .quoted(tableIdentifier.table)
      .space
      .block(_.raw(columns.map(_.quotedName).mkString(",")))
  }

  def loadDataQuery(tableFragment: QueryFragment, extension: String, onDupKeyStr: String): QueryFragment = {
    QueryFragment()
      .raw(s"LOAD DATA LOCAL INFILE '###.$extension' ")
      .raw(onDupKeyStr)
      .raw(" INTO TABLE ")
      .fragment(tableFragment)
  }

  def createDatabaseQuery(databaseName: String, ifNotExists: Boolean = true): QueryFragment = {
    QueryFragment()
      .raw(s"CREATE DATABASE ")
      .raw(if (ifNotExists) " IF NOT EXISTS " else "")
      .quoted(databaseName)
  }
}
