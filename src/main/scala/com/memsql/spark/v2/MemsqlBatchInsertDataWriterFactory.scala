package com.memsql.spark.v2

import java.sql.Connection
import java.util.Base64

import com.memsql.spark.{JdbcHelpers, LazyLogging, MemsqlOptions, WriteSuccess}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types.{BinaryType, StructType}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

class MemsqlBatchInsertDataWriterFactory(schema: StructType,
                                         partitionId: Int,
                                         attemptNumber: Int,
                                         isReferenceTable: Boolean,
                                         mode: SaveMode,
                                         table: TableIdentifier,
                                         conf: MemsqlOptions)
    extends DataWriterFactory
    with LazyLogging {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val queryPrefix = s"INSERT INTO ${table.quotedString} VALUES "
    val querySuffix = s" ON DUPLICATE KEY UPDATE ${conf.onDuplicateKeySQL.get}"

    val rowTemplate = "(" + schema
      .map(x =>
        x.dataType match {
          case BinaryType => "FROM_BASE64(?)"
          case _          => "?"
      })
      .mkString(",") + ")"
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

    def writeBatch(buff: ListBuffer[InternalRow]): Long = {
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
            rowSeq    = row.toSeq(schema)
            rowLength = rowSeq.size
            j <- 0 until rowLength
          } rowSeq(j) match {
            case bytes: Array[Byte] =>
              stmt.setObject(i * rowLength + j + 1, Base64.getEncoder.encode(bytes))
            case obj =>
              stmt.setObject(i * rowLength + j + 1, obj)
          }
          stmt.executeUpdate()
        } finally {
          stmt.close()
          conn.commit()
        }
      }
    }

    new BatchInsertWriter(conf.insertBatchSize, writeBatch, conn, schema)
  }
}

class BatchInsertWriter(batchSize: Int,
                        writeBatch: ListBuffer[InternalRow] => Long,
                        conn: Connection,
                        schema: StructType)
    extends DataWriter[InternalRow] {
  var buff: ListBuffer[InternalRow] = ListBuffer.empty[InternalRow]

  override def write(row: InternalRow): Unit = {
    buff += row
    if (buff.size >= batchSize) {
      writeBatch(buff)
      buff = ListBuffer.empty[InternalRow]
    }
  }

  override def commit(): WriterCommitMessage = {
    try {
      writeBatch(buff)
      buff = ListBuffer.empty[InternalRow]
    } finally {
      conn.close()
    }
    new WriteSuccess
  }

  override def abort(): Unit = {
    buff = ListBuffer.empty[InternalRow]
    conn.abort(ExecutionContext.global)
    conn.close()
  }

  override def close(): Unit = {
    conn.close()
  }
}
