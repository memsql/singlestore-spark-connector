package com.memsql.spark.v2

import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.util.Base64

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}

import scala.concurrent.Future

class MemsqlLoadDataWriterFactory extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
//    new MemsqlLoadDataWriter()
    null
}

class MemsqlLoadDataWriter(outputstream: OutputStream, writeFuture: Future[Long], conn: Connection)
    extends DataWriter[InternalRow] {

  override def write(row: InternalRow): Unit = {
    val rowLength = row.numFields
    for (i <- 0 until rowLength) {

      // We tried using off the shelf CSVWriter, but found it qualitatively slower.
      // The csv writer below has been benchmarked at 90 Mps going to a null output stream
//      val value = row(i) match {
//        case null => "\\N".getBytes(StandardCharsets.UTF_8)
//        // NOTE: We special case booleans because MemSQL/MySQL's LOAD DATA
//        // semantics only accept "1" as true in boolean/tinyint(1) columns
//        case true               => "1".getBytes(StandardCharsets.UTF_8)
//        case false              => "0".getBytes(StandardCharsets.UTF_8)
//        case bytes: Array[Byte] => Base64.getEncoder.encode(bytes)
//        case rawValue => {
//          var valueString = rawValue.toString
//
//          if (valueString.indexOf('\\') != -1) {
//            valueString = valueString.replace("\\", "\\\\")
//          }
//          if (valueString.indexOf('\n') != -1) {
//            valueString = valueString.replace("\n", "\\n")
//          }
//          if (valueString.indexOf('\t') != -1) {
//            valueString = valueString.replace("\t", "\\t")
//          }
//
//          valueString.getBytes(StandardCharsets.UTF_8)
//        }
//      }
      outputstream.write(null)
//      outputstream.write(value)
      outputstream.write(if (i < rowLength - 1) '\t' else '\n')
    }
  }

  override def commit(): WriterCommitMessage = ???

  override def abort(): Unit = ???

  override def close(): Unit = ???
}
