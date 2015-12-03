package com.memsql.spark.connector.util

import java.net.InetAddress

import org.apache.spark.TaskContext

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

  /**
    * Determine if the MemSQL node referred to by this object is colocated
    * with the machine you run this function on.
    *
    * @note The method used in this function is currently VERY brittle
    *       and will not work in plenty of valid cases.
    */
  def isColocated: Boolean = Option(TaskContext.get) match {
    case Some(tc) => tc.taskMetrics().hostname == dbHost
    case None => InetAddress.getLocalHost.getHostName == dbHost
  }
}
