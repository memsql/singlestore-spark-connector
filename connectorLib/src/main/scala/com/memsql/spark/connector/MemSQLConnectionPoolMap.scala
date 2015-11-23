package com.memsql.spark.connector

import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap

import com.memsql.spark.connector.util.MemSQLConnectionInfo
import org.apache.commons.pool2.impl.GenericObjectPool

class MemSQLConnectionWrapper(val conn: Connection,
                              val key: (String, Int, String)) {}

object MemSQLConnectionPoolMap {
  var poolMap: ConcurrentHashMap[(String, Int, String), GenericObjectPool[Connection]] = new ConcurrentHashMap

  def apply(info: MemSQLConnectionInfo): MemSQLConnectionWrapper =
    MemSQLConnectionPoolMap(info.dbHost, info.dbPort, info.user, info.password, info.dbName)

  def apply(dbHost: String,
            dbPort: Int,
            user: String,
            password: String,
            dbName: String): MemSQLConnectionWrapper = {
    val key = (dbHost, dbPort, dbName)
    val memSQLPooledObjectFactory = new MemSQLPooledObjectFactory(dbHost, dbPort, user, password, dbName)
    val newPool = new GenericObjectPool[Connection](memSQLPooledObjectFactory)
    poolMap.putIfAbsent(key, newPool)
    new MemSQLConnectionWrapper(poolMap.get(key).borrowObject, (dbHost, dbPort, dbName))
  }

  def returnConnection(wrapper: MemSQLConnectionWrapper): Unit = {
    poolMap.get(wrapper.key).returnObject(wrapper.conn)
  }
}
