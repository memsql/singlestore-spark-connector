package org.apache.spark.sql.memsql

import com.memsql.spark.connector.sql.QueryFragment
import com.memsql.spark.connector.util.MemSQLConnectionInfo
import org.apache.spark.sql.Row

abstract class IngestStrategy {
  def tableFragment: QueryFragment
  def conf: SaveToMemSQLConf

  def loadPartition(connInfo: MemSQLConnectionInfo, partition: Iterator[Row]): Long
}
