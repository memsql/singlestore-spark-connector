package com.memsql.spark.connector.sql

case class TableIdentifier(table: String, database: Option[String] = None) {
  def withDatabase(database: String): TableIdentifier = this.copy(database = Some(database))

  override def toString: String = quotedString

  def quotedString: String =
    (database.toSeq :+ table).map("`" + _ + "`").mkString(".")
}

object TableIdentifier {
  def apply(databaseName: String, tableName: String): TableIdentifier =
    TableIdentifier(tableName, Some(databaseName))
}
