package com.singlestore.spark

import java.sql.Connection
import java.util.Properties

import com.singlestore.spark.JdbcHelpers.getDDLConnProperties
import com.singlestore.spark.SQLGen.VariableList
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerApplicationEnd,
  SparkListenerStageCompleted,
  SparkListenerStageSubmitted
}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

class AggregatorParallelReadListener(applicationId: String) extends SparkListener with LazyLogging {
  // connectionsMap is a map from the result table name to the connection with which this table was created
  private val connectionsMap: mutable.Map[String, Connection] =
    new mutable.HashMap[String, Connection]()

  // rddInfos is a map from RDD id to the info needed to create result table for this RDD
  private val rddInfos: mutable.Map[Int, SingleStoreRDDInfo] =
    new mutable.HashMap[Int, SingleStoreRDDInfo]()

  // SingleStoreRDDInfo is information needed to create a result table
  private case class SingleStoreRDDInfo(query: String,
                                        variables: VariableList,
                                        schema: StructType,
                                        connectionProperties: Properties,
                                        materialized: Boolean,
                                        needsRepartition: Boolean,
                                        repartitionColumns: Seq[String])

  def addRDDInfo(rdd: SinglestoreRDD): Unit = {
    rddInfos.synchronized({
      rddInfos += (rdd.id -> SingleStoreRDDInfo(
        rdd.query,
        rdd.variables,
        rdd.schema,
        getDDLConnProperties(rdd.options, isOnExecutor = false),
        rdd.parallelReadType.contains(ReadFromAggregatorsMaterialized),
        rdd.options.parallelReadRepartition,
        rdd.parallelReadRepartitionColumns,
      ))
    })
  }

  def deleteRDDInfo(rdd: SinglestoreRDD): Unit = {
    rddInfos.synchronized({
      rddInfos -= rdd.id
    })
  }

  def isEmpty: Boolean = {
    rddInfos.synchronized({
      rddInfos.isEmpty
    })
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    log.info("ENDING")
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    stageSubmitted.stageInfo.rddInfos.foreach(rddInfo => {
      if (rddInfo.name == "SinglestoreRDD") {
        rddInfos
          .synchronized(
            rddInfos.get(rddInfo.id)
          )
          .foreach(singleStoreRDDInfo => {
            val stageId   = stageSubmitted.stageInfo.stageId
            val tableName = JdbcHelpers.getResultTableName(applicationId, stageId, rddInfo.id)

            // Create connection and save it in the map
            val conn =
              SinglestoreConnectionPool.getConnection(singleStoreRDDInfo.connectionProperties)
            connectionsMap.synchronized(
              connectionsMap += (tableName -> conn)
            )

            // Create result table
            JdbcHelpers.createResultTable(
              conn,
              tableName,
              singleStoreRDDInfo.query,
              singleStoreRDDInfo.schema,
              singleStoreRDDInfo.variables,
              singleStoreRDDInfo.materialized,
              singleStoreRDDInfo.needsRepartition,
              singleStoreRDDInfo.repartitionColumns
            )
          })
      }
    })
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stageCompleted.stageInfo.rddInfos.foreach(rddInfo => {
      if (rddInfo.name == "SinglestoreRDD") {
        val stageId   = stageCompleted.stageInfo.stageId
        val tableName = JdbcHelpers.getResultTableName(applicationId, stageId, rddInfo.id)

        connectionsMap.synchronized(
          connectionsMap
            .get(tableName)
            .foreach(conn => {
              // Drop result table
              JdbcHelpers.dropResultTable(conn, tableName)
              // Close connection
              conn.close()
              // Delete connection from map
              connectionsMap -= tableName
            })
        )
      }
    })
  }
}

case object AggregatorParallelReadListenerAdder {
  // listeners is a map from SparkContext hash code to the listener associated with this SparkContext
  private val listeners = new mutable.HashMap[SparkContext, AggregatorParallelReadListener]()

  def addRDD(rdd: SinglestoreRDD): Unit = {
    this.synchronized({
      val listener = listeners.getOrElse(
        rdd.sparkContext, {
          val newListener = new AggregatorParallelReadListener(rdd.sparkContext.applicationId)
          rdd.sparkContext.addSparkListener(newListener)
          listeners += (rdd.sparkContext -> newListener)
          newListener
        }
      )
      listener.addRDDInfo(rdd)
    })
  }

  def deleteRDD(rdd: SinglestoreRDD): Unit = {
    this.synchronized({
      listeners
        .get(rdd.sparkContext)
        .foreach(listener => {
          listener.deleteRDDInfo(rdd)
          if (listener.isEmpty) {
            listeners -= rdd.sparkContext
            rdd.sparkContext.removeSparkListener(listener)
          }
        })
    })
  }
}
