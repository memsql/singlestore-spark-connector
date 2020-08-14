package com.memsql.spark.v2

import java.io.OutputStream
import java.sql.Connection

import com.memsql.spark._
import org.apache.avro.Schema
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.types.StructType

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

case class MemsqlLoadDataWriteBuilder(schema: StructType,
                                      partitionId: Int,
                                      attemptNumber: Int,
                                      isReferenceTable: Boolean,
                                      mode: SaveMode,
                                      table: TableIdentifier,
                                      conf: MemsqlOptions)
    extends WriteBuilder
    with LazyLogging {

  override def buildForBatch(): BatchWrite = {
    new LoadDataWriter(schema, partitionId, attemptNumber, isReferenceTable, mode, table, conf)
  }

  //TODO implement writer for streaming
  override def buildForStreaming(): StreamingWrite = super.buildForStreaming()
}

class LoadDataWriter(schema: StructType,
                     partitionId: Int,
                     attemptNumber: Int,
                     isReferenceTable: Boolean,
                     mode: SaveMode,
                     table: TableIdentifier,
                     conf: MemsqlOptions)
    extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new MemsqlLoadDataWriterFactory(schema,
                                    partitionId,
                                    attemptNumber,
                                    isReferenceTable,
                                    mode,
                                    table,
                                    conf)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

}

class AvroDataWriter(avroSchema: Schema,
                     outputstream: OutputStream,
                     writeFuture: Future[Long],
                     conn: Connection)
    extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = ???

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    outputstream.close()
    Await.result(writeFuture, Duration.Inf)
    new WriteSuccess
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    conn.abort(ExecutionContext.global)
    outputstream.close()
    Await.ready(writeFuture, Duration.Inf)
  }
}
