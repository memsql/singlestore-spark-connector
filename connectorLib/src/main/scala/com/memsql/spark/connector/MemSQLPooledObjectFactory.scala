package com.memsql.spark.connector

import java.sql.Connection

import com.memsql.spark.context.MemSQLContext
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.commons.pool2.impl.DefaultPooledObject

class MemSQLPooledObjectFactory(val host: String,
                                val port: Int,
                                val user: String,
                                val password: String,
                                val database: String) extends BasePooledObjectFactory[Connection] {

  def create: Connection = {
    Class.forName("com.mysql.jdbc.Driver").newInstance
    MemSQLContext.getMemSQLConnection(host, port, user, password, database)
  }

  def wrap(conn: Connection): PooledObject[Connection] = {
    new DefaultPooledObject[Connection](conn)
  }
}
