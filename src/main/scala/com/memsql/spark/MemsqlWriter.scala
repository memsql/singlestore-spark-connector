package com.memsql.spark

import java.io.{InputStream, OutputStream, PipedInputStream, PipedOutputStream}
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.util.zip.GZIPOutputStream

import com.memsql.spark.MemsqlOptions.CompressionType
import net.jpountz.lz4.LZ4FrameOutputStream
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

case class PartitionWriteSuccess() extends WriterCommitMessage

class PartitionWriterFactory(table: TableIdentifier, conf: MemsqlOptions)
    extends Serializable
    with LazyLogging {

  final val BUFFER_SIZE = 524288

  type ImplementsSetInfileStream = {
    def setLocalInfileInputStream(input: InputStream)
  }

  def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    val basestream  = new PipedOutputStream
    val inputstream = new PipedInputStream(basestream, BUFFER_SIZE)

    val (ext, outputstream) = conf.loadDataCompression match {
      case CompressionType.GZip =>
        // With gzip default 1 we get a 50% improvement in bandwidth
        // (up to 16 Mps) over gzip default 6 on customer workload.
        //
        ("gz", new GZIPOutputStream(basestream) { { `def`.setLevel(1) } })

      case CompressionType.LZ4 =>
        ("lz4", new LZ4FrameOutputStream(basestream))

      case CompressionType.Skip =>
        ("tsv", basestream)
    }

    val query =
      s"LOAD DATA LOCAL INFILE '###.$ext' INTO TABLE ${table.quotedString}"

    val conn = JdbcUtils.createConnectionFactory(
      JdbcHelpers.getDMLJDBCOptions(conf)
    )()

    val writer = Future[Long] {
      try {
        val stmt = conn.createStatement()
        try {
          stmt
            .asInstanceOf[ImplementsSetInfileStream]
            .setLocalInfileInputStream(inputstream)

          log.debug(s"Executing SQL:\n$query")
          stmt.executeUpdate(query)
        } finally {
          stmt.close()
        }
      } finally {
        conn.close()
      }
    }

    new PartitionWriter(outputstream, writer, conn)
  }
}

class PartitionWriter(outputstream: OutputStream, writeFuture: Future[Long], conn: Connection)
    extends DataWriter[Row] {

  override def write(row: Row): Unit = {
    val rowLength = row.size
    for (i <- 0 until rowLength) {
      // We tried using off the shelf CSVWriter, but found it qualitatively slower.
      // The csv writer below has been benchmarked at 90 Mps going to a null output stream
      val value = row(i) match {
        case null => "\\N"
        // NOTE: We special case booleans because MemSQL/MySQL's LOAD DATA
        // semantics only accept "1" as true in boolean/tinyint(1) columns
        case true  => "1"
        case false => "0"
        case rawValue => {
          var valueString = rawValue.toString
          if (valueString.indexOf('\\') != -1) {
            valueString = valueString.replace("\\", "\\\\")
          }
          if (valueString.indexOf('\n') != -1) {
            valueString = valueString.replace("\n", "\\n")
          }
          if (valueString.indexOf('\t') != -1) {
            valueString = valueString.replace("\t", "\\t")
          }

          valueString
        }
      }

      outputstream.write(value.getBytes(StandardCharsets.UTF_8))
      outputstream.write(if (i < rowLength - 1) '\t' else '\n')
    }
  }

  override def commit(): WriterCommitMessage = {
    outputstream.close()
    Await.result(writeFuture, Duration.Inf)
    new PartitionWriteSuccess
  }

  override def abort(): Unit = {
    conn.abort(ExecutionContext.global)
    outputstream.close()
    Await.ready(writeFuture, Duration.Inf)
  }
}
