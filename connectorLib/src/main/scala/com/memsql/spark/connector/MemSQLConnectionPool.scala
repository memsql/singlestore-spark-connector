package com.memsql.spark.connector

import java.util.concurrent.ConcurrentHashMap

import com.memsql.spark.connector.util.{Loan, MemSQLConnectionInfo}
import java.sql.{DriverManager, Connection}

import org.apache.commons.dbcp2.BasicDataSource

object MemSQLConnectionPool {
  val DEFAULT_JDBC_LOGIN_TIMEOUT = 10 //seconds
  val pools: ConcurrentHashMap[MemSQLConnectionInfo, BasicDataSource] = new ConcurrentHashMap

  def createPool(info: MemSQLConnectionInfo): BasicDataSource = {
    DriverManager.setLoginTimeout(DEFAULT_JDBC_LOGIN_TIMEOUT)

    val newPool = new BasicDataSource
    newPool.setDriverClassName("com.mysql.jdbc.Driver")
    newPool.setUrl(info.toJDBCAddress)
    newPool.setUsername(info.user)
    newPool.setPassword(info.password)
    newPool.addConnectionProperty("zeroDateTimeBehavior", "convertToNull")

    newPool
  }

  def connect(info: MemSQLConnectionInfo): Connection = {
    if (!pools.contains(info)) {
      val newPool = createPool(info)
      pools.putIfAbsent(info, newPool)
    }
    pools.get(info).getConnection
  }

  def withConnection[T](info: MemSQLConnectionInfo)(handle: Connection => T): T =
    Loan[Connection](connect(info)).to(handle)
}
