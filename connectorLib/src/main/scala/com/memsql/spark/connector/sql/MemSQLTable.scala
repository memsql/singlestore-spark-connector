package com.memsql.spark.connector.sql

case class MemSQLTable(tableIdent: TableIdentifier,
                       columns: Seq[ColumnDefinition],
                       keys: Seq[MemSQLKey],
                       ifNotExists: Boolean = false) {

  if (columns.isEmpty) {
    throw new IllegalArgumentException("`columns` must not be empty.")
  }

  def quotedName: String = tableIdent.quotedString

  def ifNotExistsExpr: String = if (ifNotExists) { "IF NOT EXISTS"} else { "" }

  def hasShardKey: Boolean = keys.exists(_.canBeUsedAsShardKey)

  def toSQL: String = {
    // If we don't have a shard key, add an empty one
    val tableKeys = if (hasShardKey) { keys } else { keys :+ Shard() }

    QueryFragment()
      .raw("CREATE TABLE")
      .space
      .raw(ifNotExistsExpr)
      .space
      .raw(quotedName)
      .space
      .block { inner =>
        // Add the column definitions
        inner.addFragments(columns.map(_.toQueryFragment), ", ")
             .raw(", ")

        // Add the keys
        inner.raw(tableKeys.map(_.toSQL).mkString(", "))
      }
      .sql
      .toString
  }
}
