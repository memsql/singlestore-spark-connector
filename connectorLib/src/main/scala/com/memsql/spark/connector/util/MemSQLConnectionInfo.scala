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
    case Some(tc) => false
      /* todo: We need a way to get the executor hostname and they removed it from
       * taskmetrics(). We can potentially use utils.getHostName() but it is private so it would require moving this
       * to org.apache.spark package
       */
    case None => InetAddress.getLocalHost.getHostName == dbHost
  }
}
