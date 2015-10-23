package com.memsql.spark.connector

import java.sql.{DriverManager, Connection}

import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.commons.pool2.impl.DefaultPooledObject

class MemSQLPooledObjectFactory(val host: String,
                                val port: Int,
                                val user: String,
                                val password: String,
                                val schema: String) extends BasePooledObjectFactory[Connection] {

  def create: Connection = {
    Class.forName("com.mysql.jdbc.Driver").newInstance
    val url: String = "jdbc:mysql://" + host + ":" + port + "/" + schema
    return DriverManager.getConnection(url, user, password)
  }

  def wrap(conn: Connection): PooledObject[Connection] = {
    return new DefaultPooledObject[Connection](conn)
  }
}
