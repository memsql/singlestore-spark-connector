package com.memsql.spark

import java.sql.Connection

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

class BatchInsertWriterFactory(table: TableIdentifier, conf: MemsqlOptions)
    extends WriterFactory
    with LazyLogging {

  def createDataWriter(schema: StructType,
                       partitionId: Int,
                       attemptNumber: Int,
                       isReferenceTable: Boolean,
                       mode: SaveMode): DataWriter[Row] = {
    val queryPrefix = s"INSERT INTO ${table.quotedString} VALUES "
    val querySuffix = s" ON DUPLICATE KEY UPDATE ${conf.onDuplicateKeySQL.get}"

    val rowTemplate = "(" + ("?" * schema.length).mkString(",") + ")"
    def valueTemplate(rows: Int): String =
      List.fill(rows)(rowTemplate).mkString(",")
    val fullBatchQuery = queryPrefix + valueTemplate(conf.insertBatchSize) + querySuffix

    val conn = JdbcUtils.createConnectionFactory(
      if (isReferenceTable) {
        JdbcHelpers.getDDLJDBCOptions(conf)
      } else {
        JdbcHelpers.getDMLJDBCOptions(conf)
      }
    )()
    conn.setAutoCommit(false)

    def writeBatch(buff: ListBuffer[Row]): Long = {
      if (buff.isEmpty) {
        0
      } else {
        val rowsCount = buff.size
        val query = if (rowsCount == conf.insertBatchSize) {
          fullBatchQuery
        } else {
          queryPrefix + valueTemplate(rowsCount) + querySuffix
        }

        val stmt = conn.prepareStatement(query)
        try {
          for {
            (row, i) <- buff.iterator.zipWithIndex
            rowLength = row.size
            j <- 0 until rowLength
          } stmt.setObject(i * rowLength + j + 1, row(j))
          stmt.executeUpdate()
        } finally {
          stmt.close()
          conn.commit()
        }
      }
    }

    new BatchInsertWriter(conf.insertBatchSize, writeBatch, conn)
  }
}

class BatchInsertWriter(batchSize: Int, writeBatch: ListBuffer[Row] => Long, conn: Connection)
    extends DataWriter[Row] {
  var buff: ListBuffer[Row] = ListBuffer.empty[Row]

  override def write(row: Row): Unit = {
    buff += row
    if (buff.size >= batchSize) {
      writeBatch(buff)
      buff = ListBuffer.empty[Row]
    }
  }

  override def commit(): WriterCommitMessage = {
    try {
      writeBatch(buff)
      buff = ListBuffer.empty[Row]
    } finally {
      conn.close()
    }
    new WriteSuccess
  }

  override def abort(): Unit = {
    buff = ListBuffer.empty[Row]
    conn.abort(ExecutionContext.global)
    conn.close()
  }
}
