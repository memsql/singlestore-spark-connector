package com.memsql.spark.v2

import com.memsql.spark._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types.StructType

case class MemsqlWriteBuilder(schema: StructType,
                              partitionId: Int,
                              attemptNumber: Int,
                              isReferenceTable: Boolean,
                              mode: SaveMode,
                              table: TableIdentifier,
                              conf: MemsqlOptions)
    extends WriteBuilder
    with SupportsTruncate
    with LazyLogging {
  override def truncate(): WriteBuilder = {
    val conn = JdbcUtils.createConnectionFactory(
      if (isReferenceTable) {
        JdbcHelpers.getDDLJDBCOptions(conf)
      } else {
        JdbcHelpers.getDMLJDBCOptions(conf)
      }
    )()

    JdbcHelpers.truncateTable(conn, table)
    this
  }

  override def buildForBatch(): BatchWrite = {
    new MemsqlBatchWrite(schema, partitionId, attemptNumber, isReferenceTable, mode, table, conf)
  }

  //TODO implement writer for streaming
  override def buildForStreaming(): StreamingWrite = super.buildForStreaming()
}

class MemsqlBatchWrite(schema: StructType,
                       partitionId: Int,
                       attemptNumber: Int,
                       isReferenceTable: Boolean,
                       mode: SaveMode,
                       table: TableIdentifier,
                       conf: MemsqlOptions)
    extends BatchWrite
    with LazyLogging {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    log.debug(conf.toString + "!!!!!!!!!!!!!!")
    if (conf.onDuplicateKeySQL.isEmpty) {
      new MemsqlLoadDataWriterFactory(schema,
                                      partitionId,
                                      attemptNumber,
                                      isReferenceTable,
                                      mode,
                                      table,
                                      conf)
    } else {
      new MemsqlBatchInsertDataWriterFactory(schema,
                                             partitionId,
                                             attemptNumber,
                                             isReferenceTable,
                                             mode,
                                             table,
                                             conf)
    }
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

}
