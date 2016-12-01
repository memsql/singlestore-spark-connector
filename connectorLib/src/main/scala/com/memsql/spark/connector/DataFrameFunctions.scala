package com.memsql.spark.connector

import com.memsql.spark.SaveToMemSQLException
import com.memsql.spark.connector.sql.{QueryFragments, TableIdentifier}
import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.types.BinaryType

import scala.util.Random

class DataFrameFunctions(df: DataFrame) {
  def getMemSQLConf: MemSQLConf = {
    df.sqlContext match {
      //case msc: MemSQLContext => msc.memSQLConf
      case sc: SQLContext => MemSQLConf(sc.sparkContext.getConf)
    }
  }

  def getMemSQLCluster: MemSQLCluster = MemSQLCluster(getMemSQLConf)

  def saveToMemSQL(tableName: String): Long =
    saveToMemSQL(TableIdentifier(tableName), SaveToMemSQLConf(getMemSQLConf))

  def saveToMemSQL(databaseName: String, tableName: String): Long =
    saveToMemSQL(TableIdentifier(databaseName, tableName), SaveToMemSQLConf(getMemSQLConf))

  def saveToMemSQL(tableIdentifier: TableIdentifier, saveConf: SaveToMemSQLConf): Long = {
    val sparkContext = df.sqlContext.sparkContext
    /* TODO: Spark 2.0 changes the way that we interact with the cluster
     * SparkContext + SQLContext is no longer the way to interact with Spark - you have to use SparkSession
     * https://databricks.com/blog/2016/08/15/how-to-use-sparksession-in-apache-spark-2-0.html
     * We should fix this everywhere
     */
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val cluster = getMemSQLCluster
    val columns = df.schema.toMemSQLColumns
    val tableFragment = QueryFragments.tableNameWithColumns(tableIdentifier, columns.map(_.reference))
    val dbName = tableIdentifier.database.getOrElse(cluster.conf.defaultDBName)

    // Since LOAD DATA ... ON DUPLICATE KEY UPDATE is not currently supported by MemSQL
    // we fallback to batch insert if a dup key expression is specified.
    // Also, if the DataFrame does not have any columns (e.g. we are
    // inserting empty rows and relying on columns' default values), we need
    // to use INSERT, since LOAD DATA doesn't support empty rows.
    val useInsert = df.schema.size == 0 ||
      saveConf.onDuplicateKeySQL.isDefined ||
      df.schema.exists(_.dataType == BinaryType)

    val ingestStrategy = {
      if (useInsert) {
        InsertStrategy(tableFragment, saveConf)
      } else {
        LoadDataStrategy(tableFragment, saveConf)
      }
    }

    if (saveConf.createMode == CreateMode.DatabaseAndTable) {
      cluster.createDatabase(tableIdentifier)
    }
    if (saveConf.createMode == CreateMode.DatabaseAndTable || saveConf.createMode == CreateMode.Table) {
      cluster.createTable(tableIdentifier, columns ++ saveConf.extraColumns, saveConf.extraKeys)
    }

    val availableNodes = saveConf.useKeylessShardingOptimization match {
      case true => cluster.getMasterPartitions(dbName)
      case false => cluster.getAggregators.map(info => info.copy(dbName=dbName))
    }

    val numRowsAccumulator = sparkContext.accumulator[Long](0L, "DataFrame.saveToMemSQL")

    // Each time we run saveToMemSQL, pick nodes from
    // the available set starting at this point
    //
    val randomStart = Random.nextInt(availableNodes.length)

    try {
      if (saveConf.dryRun) {
        df.map(x => 0).reduce(_ + _)
      } else {
        df.foreachPartition(partition => {
          val partitionId = TaskContext.getPartitionId

          val targetNode = availableNodes.filter(_.isColocated) match {
            case s@(_ :: _) => s(Random.nextInt(s.length))
            case Nil => {
              val choice = (randomStart + partitionId) % availableNodes.length
              availableNodes(choice)
            }
          }

          numRowsAccumulator += ingestStrategy.loadPartition(targetNode, partition)
        })
      }
    } catch {
      case e: SparkException => throw new SaveToMemSQLException(e, numRowsAccumulator.value)
    }
    numRowsAccumulator.value
  }

}