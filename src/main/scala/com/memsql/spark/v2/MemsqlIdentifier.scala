package com.memsql.spark.v2

import org.apache.spark.sql.connector.catalog.Identifier

class MemsqlIdentifier extends Identifier {

  override def namespace(): Array[String] = {
    Array("testdb")
  }

  override def name(): String = {
    "MemsqlIdentifier"
  }
}
