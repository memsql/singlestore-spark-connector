package com.singlestore.spark

import java.sql.Connection
import java.util.Properties
import org.apache.commons.dbcp2.{BasicDataSource, BasicDataSourceFactory}
import scala.collection.mutable

object SinglestoreConnectionPool {
  private var dataSources = new mutable.HashMap[Properties, BasicDataSource]()

  private def deleteEmptyDataSources(): Unit = {
    dataSources = dataSources.filter(pair => {
      val dataSource = pair._2
      if (dataSource.getNumActive + dataSource.getNumIdle == 0) {
        dataSource.close()
        false
      } else {
        true
      }
    })
  }

  def getConnection(properties: Properties): Connection = {
    this.synchronized({
      dataSources
        .getOrElse(
          properties, {
            deleteEmptyDataSources()
            val newDataSource = BasicDataSourceFactory.createDataSource(properties)
            newDataSource.addConnectionProperty("connectionAttributes", 
                                                properties.getProperty("connectionAttributes"))
            dataSources += (properties -> newDataSource)
            newDataSource
          }
        )
        .getConnection
    })
  }

  def close(): Unit = {
    this.synchronized({
      dataSources.foreach(pair => pair._2.close())
      dataSources = new mutable.HashMap[Properties, BasicDataSource]()
    })
  }
}
