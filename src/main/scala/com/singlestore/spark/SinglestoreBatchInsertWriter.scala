package com.singlestore.spark

import java.sql.Connection
import java.util.Base64

import com.singlestore.spark.JdbcHelpers.{getClusterConnProperties, getAdminConnProperties}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.sql.{Row, SaveMode}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

// TODO: extend it from DataWriterFactory
class BatchInsertWriterFactory(table: TableIdentifier, conf: SinglestoreOptions)
    extends WriterFactory
    with LazyLogging {

  def createDataWriter(schema: StructType,
                       partitionId: Int,
                       attemptNumber: Int,
                       isReferenceTable: Boolean,
                       mode: SaveMode): DataWriter[Row] = {
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

    val conn = SinglestoreConnectionPool.getConnection(
      if (isReferenceTable && !conf.version.atLeast("7.5.0")) {
        JdbcHelpers.getAdminConnProperties(conf, isOnExecutor = true) match {
          case Some(properties) => properties
          case None =>
            throw new IllegalArgumentException(
              s"Table ${table.quotedString} is a reference table.\n" +
                "Admin endpoint is required to write data to the reference table.\n" +
                "Please, provide endpoint to Master Aggregator using adminEndpoint option.")
        }
      } else {
        JdbcHelpers.getClusterConnProperties(conf, isOnExecutor = true)
      })
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
          } row(j) match {
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
