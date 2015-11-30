package com.memsql.spark.connector.util

case class MemSQLConnectionInfo(dbHost: String,
                                dbPort: Int,
                                user: String,
                                password: String,
                                dbName: String) {

  def toJDBCAddress: String = {
    var address = s"jdbc:mysql://$dbHost:$dbPort"
    if (dbName.length > 0) {
      address += "/" + dbName
    }
    address
  }
}
