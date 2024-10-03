package com.singlestore.spark

import java.sql.{Connection, SQLException}
import java.util.Properties
import com.singlestore.spark.JdbcHelpers.getDDLConnProperties
import com.singlestore.spark.SQLGen.VariableList
import org.apache.spark.{DataSourceTelemetryHelpers, SparkContext}
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerStageCompleted,
  SparkListenerStageSubmitted
}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

class AggregatorParallelReadListener(applicationId: String)
  extends SparkListener
    with LazyLogging
    with DataSourceTelemetryHelpers {
  // connectionsMap is a map from the result table name to the connection with which this table was created
  private val connectionsMap: mutable.Map[String, Connection] =
    new mutable.HashMap[String, Connection]()

  // rddInfos is a map from RDD id to the info needed to create result table for this RDD
  private val rddInfos: mutable.Map[Int, SingleStoreRDDInfo] =
    new mutable.HashMap[Int, SingleStoreRDDInfo]()

  // SingleStoreRDDInfo is information needed to create a result table
  private case class SingleStoreRDDInfo(sc: SparkContext,
                                        query: String,
                                        variables: VariableList,
                                        schema: StructType,
                                        connectionProperties: Properties,
                                        materialized: Boolean,
                                        needsRepartition: Boolean,
                                        repartitionColumns: Seq[String])

  def addRDDInfo(rdd: SinglestoreRDD): Unit = {
    rddInfos.synchronized({
      rddInfos += (rdd.id -> SingleStoreRDDInfo(
        rdd.sparkContext,
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

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    stageSubmitted.stageInfo.rddInfos.foreach(rddInfo => {
      if (rddInfo.name.startsWith("SingleStoreRDD")) {
        rddInfos
          .synchronized(
            rddInfos.get(rddInfo.id)
          )
          .foreach(singleStoreRDDInfo => {
            val stageId       = stageSubmitted.stageInfo.stageId
            val attemptNumber = stageSubmitted.stageInfo.attemptNumber()
            val randHex       = rddInfo.name.substring("SingleStoreRDD".size)
            val tableName =
              JdbcHelpers
                .getResultTableName(applicationId, stageId, rddInfo.id, attemptNumber, randHex)

            // Create connection and save it in the map
            val conn =
              SinglestoreConnectionPool.getConnection(singleStoreRDDInfo.connectionProperties)
            connectionsMap.synchronized(
              connectionsMap += (tableName -> conn)
            )

            log.info(logEventNameTagger(s"Creating result table '$tableName'"))
            try {
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
              log.info(logEventNameTagger(s"Successfully created result table '$tableName'"))
            } catch {
              // Cancel execution if we failed to create a result table
              case e: SQLException => {
                singleStoreRDDInfo.sc.cancelStage(stageId)
                throw e
              }
            }
          })
      }
    })
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stageCompleted.stageInfo.rddInfos.foreach(rddInfo => {
      if (rddInfo.name.startsWith("SingleStoreRDD")) {
        val stageId       = stageCompleted.stageInfo.stageId
        val attemptNumber = stageCompleted.stageInfo.attemptNumber()
        val randHex       = rddInfo.name.substring("SingleStoreRDD".size)
        val tableName =
          JdbcHelpers.getResultTableName(applicationId, stageId, rddInfo.id, attemptNumber, randHex)

        connectionsMap.synchronized(
          connectionsMap
            .get(tableName)
            .foreach(conn => {
              // Drop result table
              log.info(logEventNameTagger(s"Dropping result table '$tableName'"))
              JdbcHelpers.dropResultTable(conn, tableName)
              log.info(logEventNameTagger(s"Successfully dropped result table '$tableName'"))
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
