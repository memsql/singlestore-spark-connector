package com.memsql.spark.connector.util

import com.memsql.spark.context.MemSQLNode

case class MemSQLConnectionInfo(dbHost: String,
                                dbPort: Int,
                                user: String,
                                password: String,
                                dbName: String) {

  override def equals(obj: scala.Any): Boolean = obj match {
    case n: MemSQLNode => (n.host == dbHost && n.port == dbPort)
    case c: MemSQLConnectionInfo =>
      dbHost == c.dbHost &&
      dbPort == c.dbPort &&
      user == c.user &&
      password == c.password &&
      dbName == c.dbName
    case _ => super.equals(obj)
  }
}
