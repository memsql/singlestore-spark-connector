package com.memsql.spark.connector

import com.memsql.spark.connector.sql.{InsertQuery, QueryFragment}
import com.memsql.spark.connector.util.MemSQLConnectionInfo
import org.apache.spark.sql.Row

case class InsertStrategy(tableFragment: QueryFragment,
                          conf: SaveToMemSQLConf) extends IngestStrategy {

  override def loadPartition(connInfo: MemSQLConnectionInfo, partition: Iterator[Row]): Long = {
    MemSQLConnectionPool.withConnection(connInfo) { conn =>
      conn.setAutoCommit(false)

      val insertQuery = new InsertQuery(tableFragment, conf.saveMode, conf.onDuplicateKeySQL)

      val numRowsAffected = {
        partition
          .grouped(conf.insertBatchSize)
          .map(group => {
            val numRows = insertQuery.flush(conn, group.toList)
            conn.commit()
            numRows
          })
          .sum
      }

      numRowsAffected
    }
  }
}
