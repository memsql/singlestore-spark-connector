package com.singlestore.spark

import java.sql.Connection
import java.util.Base64

import com.singlestore.spark.JdbcHelpers.{appendTagsToQuery, getDDLConnProperties, getDMLConnProperties}
import org.apache.spark.DataSourceTelemetryHelpers
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.sql.{Row, SaveMode}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

// TODO: extend it from DataWriterFactory
class BatchInsertWriterFactory(table: TableIdentifier, conf: SinglestoreOptions)
    extends WriterFactory
    with LazyLogging
    with DataSourceTelemetryHelpers {

  def createDataWriter(schema: StructType,
                       partitionId: Int,
                       attemptNumber: Int,
                       isReferenceTable: Boolean,
                       mode: SaveMode): DataWriter[Row] = {
    val columnNames = schema.map(s => SinglestoreDialect.quoteIdentifier(s.name))
    val queryPrefix = s"INSERT INTO ${table.quotedString} (${columnNames.mkString(", ")}) VALUES "
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

    val conn = SinglestoreConnectionPool.getConnection(if (isReferenceTable) {
      getDDLConnProperties(conf, isOnExecutor = true)
    } else {
      getDMLConnProperties(conf, isOnExecutor = true)
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

        val finalQuery = appendTagsToQuery(conf, query)
        log.info(logEventNameTagger(s"Executing SQL:\n$finalQuery"))

        val stmt = conn.prepareStatement(finalQuery)
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

  override def abort(e: Exception): Unit = {
    buff = ListBuffer.empty[Row]
    if (!conn.isClosed) {
      conn.abort(ExecutionContext.global)
    }
  }
}
