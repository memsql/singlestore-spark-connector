package com.memsql.spark.connector

import com.memsql.spark.connector.dataframe.MemSQLDataFrameUtils
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.Random

import com.memsql.spark.connector.util.{Loan, MemSQLConnectionInfo}
import com.memsql.spark.connector.util.JDBCImplicits._
import java.sql.Connection

case class MemSQLCluster(conf: MemSQLConf) {
  def getMasterInfo: MemSQLConnectionInfo = conf.masterConnectionInfo

  def getRandomAggregatorInfo: MemSQLConnectionInfo = {
    val aggs = getAggregators
    aggs(Random.nextInt(aggs.size))
  }

  def withMasterConn[T]: ((Connection) => T) => T =
    MemSQLConnectionPool.withConnection(getMasterInfo)

  def withAggregatorConn[T]: ((Connection) => T) => T =
    MemSQLConnectionPool.withConnection(getRandomAggregatorInfo)

  def getAggregators: Seq[MemSQLConnectionInfo] = getAggregators(false)
  def getAggregators(includeMaster: Boolean): Seq[MemSQLConnectionInfo] = {
    val aggs = withMasterConn { conn =>
      conn.withStatement { stmt =>
        val result = stmt.executeQuery("SHOW AGGREGATORS")

        result.toIterator.flatMap(r => {
          val isMaster = r.getBoolean("Master_Aggregator")
          val isOnline = r.getString("State").equals("online")
          if (isOnline && !isMaster) {
            Some(conf.masterConnectionInfo.copy(
              dbHost=r.getString("Host"),
              dbPort=r.getInt("Port")
            ))
          } else {
            None
          }
        }).toSeq
      }
    }

    if (aggs.length == 0 || includeMaster) {
      aggs :+ conf.masterConnectionInfo
    } else {
      aggs
    }
  }

  def getQuerySchema(query: String, queryParams: Seq[Any]=Nil): StructType = {
    withMasterConn { conn =>
      val limitedQuery = s"SELECT * FROM ($query) lzalias LIMIT 0"

      val metadata = if (queryParams.isEmpty) {
        conn.withStatement(stmt =>
          stmt.executeQuery(limitedQuery).getMetaData)
      } else {
        conn.withPreparedStatement(limitedQuery, stmt => {
          stmt.fillParams(queryParams)
          stmt.executeQuery.getMetaData
        })
      }

      val numColumns = metadata.getColumnCount

      StructType(
        Range(0, numColumns)
          .map(i => StructField(
            metadata.getColumnName(i + 1),
            MemSQLDataFrameUtils.JDBCTypeToDataFrameType(metadata, i + 1),
            true)
          )
      )
    }
  }

}

