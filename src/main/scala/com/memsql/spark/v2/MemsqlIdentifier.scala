package com.memsql.spark.v2

import org.apache.spark.sql.connector.catalog.Identifier

case class MemsqlIdentifier(database: String, table: String) extends Identifier {

  override def namespace(): Array[String] = {
    Array(database)
  }

  override def name(): String = {
    table
  }
}
