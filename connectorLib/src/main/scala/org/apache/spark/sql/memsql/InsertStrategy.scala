package org.apache.spark.sql.memsql

import com.memsql.spark.connector.MemSQLConnectionPool
import com.memsql.spark.connector.sql.{InsertQuery, QueryFragment}
import com.memsql.spark.connector.util.MemSQLConnectionInfo
import org.apache.spark.sql.{SaveMode, Row}

case class InsertStrategy(tableFragment: QueryFragment,
                          conf: SaveToMemSQLConf) extends IngestStrategy {

  override def loadPartition(connInfo: MemSQLConnectionInfo, partition: Iterator[Row]): Long = {
    MemSQLConnectionPool.withConnection(connInfo) { conn =>
      conn.setAutoCommit(false)

      val insertQuery = new InsertQuery(tableFragment, conf.saveMode, conf.onDuplicateKeySQL)

      val numRowsAffected = {
        partition
          .grouped(conf.insertBatchSize)
          .map(group => insertQuery.flush(conn, group.toList))
          .sum
      }

      conn.commit()
      numRowsAffected
    }
  }
}
