package org.apache.spark.sql.memsql

import com.memsql.spark.SaveToMemSQLException
import com.memsql.spark.connector.dataframe.MemSQLDataFrameUtils
import com.memsql.spark.connector.sql.{TableIdentifier, QueryFragments, Column, MemSQLColumn}
import com.memsql.spark.connector.{MemSQLCluster, MemSQLConf}
import org.apache.spark.{TaskContext, SparkException}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, DataFrame}

import scala.util.Random

object SparkImplicits {
  implicit class SchemaFunctions(schema: StructType) {
    def toMemSQLColumns: Seq[MemSQLColumn] = {
      schema.map(s => {
        Column(
          s.name,
          MemSQLDataFrameUtils.DataFrameTypeToMemSQLTypeString(s.dataType),
          s.nullable
        )
      })
    }
  }

  implicit class DataFrameFunctions(df: DataFrame) {
    def getMemSQLConf: MemSQLConf = {
      df.sqlContext match {
        case msc: MemSQLContext => msc.memSQLConf
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
      val cluster = getMemSQLCluster
      val columns = df.schema.toMemSQLColumns
      val tableFragment = QueryFragments.tableNameWithColumns(tableIdentifier, columns)
      val dbName = tableIdentifier.database.getOrElse(cluster.conf.defaultDBName)

      // Since LOAD DATA ... ON DUPLICATE KEY UPDATE is not currently supported by MemSQL
      // we fallback to batch insert if a dup key expression is specified.
      // Also, if the DataFrame does not have any columns (e.g. we are
      // inserting empty rows and relying on columns' default values), we need
      // to use INSERT, since LOAD DATA doesn't support empty rows.
      val ingestStrategy = if (df.schema.size == 0 || saveConf.onDuplicateKeySQL.isDefined) {
        InsertStrategy(tableFragment, saveConf)
      } else {
        LoadDataStrategy(tableFragment, saveConf)
      }

      val availableNodes = saveConf.useKeylessShardingOptimization match {
        case true => cluster.getMasterPartitions(dbName)
        case false => cluster.getAggregators.map(info => info.copy(dbName=dbName))
      }

      if (saveConf.createMode == CreateMode.DatabaseAndTable) {
        cluster.createDatabase(tableIdentifier)
      }
      if (saveConf.createMode == CreateMode.DatabaseAndTable || saveConf.createMode == CreateMode.Table) {
        cluster.createTable(tableIdentifier, columns)
      }

      val numRowsAccumulator = sparkContext.accumulator[Long](0L, "DataFrame.saveToMemSQL")

      // Each time we run saveToMemSQL, pick nodes from
      // the available set starting at this point
      //
      val randomStart = Random.nextInt(availableNodes.length)

      try {
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
      } catch {
        case e: SparkException => throw new SaveToMemSQLException(e, numRowsAccumulator.value)
      }
      numRowsAccumulator.value
    }

  }
}
