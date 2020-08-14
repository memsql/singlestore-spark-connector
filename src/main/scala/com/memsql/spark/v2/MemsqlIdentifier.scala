package com.memsql.spark.v2

import org.apache.spark.sql.connector.catalog.Identifier

case class MemsqlIdentifier(database: Option[String], table: String) extends Identifier {

  override def namespace(): Array[String] = {
    Array(database.get)
  }

  override def name(): String = {
    table
  }
}
