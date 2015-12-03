package com.memsql.spark.connector

import com.memsql.spark.connector.dataframe.MemSQLDataFrameUtils
import com.memsql.spark.connector.sql._
import org.apache.spark.sql.types.{StructField, StructType}
import com.memsql.spark.connector.util.MemSQLConnectionInfo
import com.memsql.spark.connector.util.JDBCImplicits._
import java.sql.Connection
import scala.util.Random

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
  def getAggregators(includeMaster: Boolean): List[MemSQLConnectionInfo] = {
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
        }).toList
      }
    }

    if (aggs.length == 0 || includeMaster) {
      aggs :+ conf.masterConnectionInfo
    } else {
      aggs
    }
  }

  def getMasterPartitions(dbName: String): List[MemSQLConnectionInfo] = {
    withAggregatorConn(conn => {
      conn.withStatement(stmt => {
        stmt.executeQuery(s"SHOW PARTITIONS FROM $dbName")
          .toIterator
          .filter(row => {
            val role = row.getString("Role")
            role != null && role.equals("Master")
          })
          .map(row => getMasterInfo.copy(
            dbHost=row.getString("Host"),
            dbPort=row.getInt("Port"),
            dbName=s"${dbName}_${row.getInt("Ordinal")}"
          ))
          .toList
      })
    })
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

  def createDatabase(tableIdent: TableIdentifier): Unit = {
    val dbName = tableIdent.database.getOrElse(conf.defaultDBName)

    if (dbName.isEmpty) {
      throw new UnsupportedOperationException("A database name must be specified when saving data to MemSQL.")
    }

    val query = QueryFragments.createDatabaseQuery(dbName)
    val connInfo = getMasterInfo.copy(dbName="")

    MemSQLConnectionPool.withConnection(connInfo) { conn =>
      conn.withStatement(stmt => {
        stmt.execute(query.sql.toString)
      })
    }
  }

  def createTable(tableIdent: TableIdentifier, columns: Seq[MemSQLColumn]): Unit = {
    val defaultInsertColumn = AdvancedColumn("memsql_insert_time", "TIMESTAMP",
                                             false, defaultSQL=Some("CURRENT_TIMESTAMP"))

    val extraColumns = {
      if (columns.exists(c => c.name == "memsql_insert_time")) { Nil }
      else { Seq(defaultInsertColumn) }
    }
    val defaultKeys = Seq(Shard(), Key(Seq(defaultInsertColumn)))

    createTable(tableIdent, extraColumns ++ columns, defaultKeys)
  }

  def createTable(tableIdent: TableIdentifier, columns: Seq[MemSQLColumn], keys: Seq[MemSQLKey]): Unit = {
    val query = MemSQLTable(tableIdent, columns, keys, ifNotExists = true)
    withMasterConn { conn =>
      conn.withStatement(stmt => {
        stmt.execute(query.toSQL)
      })
    }
  }
}

