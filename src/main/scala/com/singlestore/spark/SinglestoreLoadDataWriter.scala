package com.singlestore.spark

import java.io.{IOException, InputStream, OutputStream, PipedInputStream, PipedOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.util.Base64
import java.util.zip.GZIPOutputStream
import com.singlestore.spark.JdbcHelpers.{appendTagsToQuery, getDDLConnProperties, getDMLConnProperties, logEventNameTagger}
import com.singlestore.spark.SinglestoreOptions.CompressionType
import com.singlestore.spark.vendor.apache.SchemaConverters
import net.jpountz.lz4.LZ4FrameOutputStream
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.commons.dbcp2.DelegatingStatement
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.sql.{Row, SaveMode}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

abstract class WriterCommitMessage extends Serializable {}
case class WriteSuccess()          extends WriterCommitMessage

abstract class DataWriter[T] {
  def write(record: T): Unit
  def commit(): WriterCommitMessage
  def abort(e: Exception): Unit
}

abstract class WriterFactory extends Serializable {
  def createDataWriter(schema: StructType,
                       partitionId: Int,
                       attemptNumber: Int,
                       isReferenceTable: Boolean,
                       mode: SaveMode): DataWriter[Row]
}

// TODO: extend it from DataWriterFactory
class LoadDataWriterFactory(table: TableIdentifier, conf: SinglestoreOptions)
    extends WriterFactory
    with LazyLogging {

  final val BUFFER_SIZE = 524288

  type ImplementsSetInfileStream = {
    def setNextLocalInfileInputStream(input: InputStream)
  }

  private def createLoadDataQuery(schema: StructType, mode: SaveMode, avroSchema: Schema): String = {
    val ext = conf.loadDataCompression match {
      case CompressionType.GZip => "gz"
      case CompressionType.LZ4 => "lz4"
      case CompressionType.Skip => "tsv"
    }

    def tempColName(colName: String) = s"@${colName}_tmp"

    val columnNames = schema.map(s =>
      if (s.dataType == BinaryType) {
        tempColName(s.name)
      } else {
        SinglestoreDialect.quoteIdentifier(s.name)
    })

    val loadDataFormat = conf.loadDataFormat
    val querySetPart =
      if (loadDataFormat == SinglestoreOptions.LoadDataFormat.Avro) ""
      else {
        val binaryColumns = schema.filter(_.dataType == BinaryType)
        if (binaryColumns.isEmpty) {
          ""
        } else {
          val operations = binaryColumns
            .map(s =>
              s"${SinglestoreDialect.quoteIdentifier(s.name)} = FROM_BASE64(${tempColName(s.name)})")
          s"SET ${operations.mkString(", ")}"
        }
      }

    val queryErrorHandlingPart = mode match {
      // If SaveMode is Ignore - skip all duplicate key errors
      case SaveMode.Ignore => "SKIP DUPLICATE KEY ERRORS"
      case _ =>
        conf.overwriteBehavior match {
          // If SaveMode is NOT Ignore and OverwriteBehavior is Merge - replace all duplicates
          case Merge => "REPLACE"
          case _     => ""
        }
    }
    val maxErrorsPart      = s"MAX_ERRORS ${conf.maxErrors}"
    val queryPrefix        = s"LOAD DATA LOCAL INFILE '###.$ext'"
    val queryEnding = if (loadDataFormat == SinglestoreOptions.LoadDataFormat.Avro) {
      val nullableSchemas = for ((field, index) <- schema.fields.zipWithIndex)
        yield
          AvroSchemaHelper.resolveNullableType(avroSchema.getFields.get(index).schema(),
                                               field.nullable)
      val avroSchemaParts = for ((field, index) <- schema.fields.zipWithIndex) yield {
        val avroSchemaMapping =
          s"${SinglestoreDialect.quoteIdentifier(field.name)} <- %::${SinglestoreDialect
            .quoteIdentifier(field.name)}"
        if (field.nullable) {
          s"$avroSchemaMapping::${nullableSchemas(index).getType.getName}"
        } else {
          avroSchemaMapping
        }
      }
      val avroMapping = avroSchemaParts.mkString("( ", ", ", " )")
      s"INTO TABLE ${table.quotedString} FORMAT AVRO $avroMapping SCHEMA '${avroSchema.toString}'"
    } else {
      s"INTO TABLE ${table.quotedString} (${columnNames.mkString(", ")})"
    }
    val query =
      List[String](queryPrefix, queryErrorHandlingPart, queryEnding, querySetPart, maxErrorsPart)
        .filter(s => !s.isEmpty)
        .mkString(" ")

    query
  }

  private def createStreams(): (InputStream, OutputStream) = {
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

    (inputstream, outputstream)
  }

  private def startStatementExecution(conn: Connection, query: String, inputstream: InputStream): Future[Long] = {
    Future[Long] {
      try {
        val stmt = conn.createStatement()
        try {
          stmt
            .asInstanceOf[DelegatingStatement]
            .getInnermostDelegate
            .asInstanceOf[ImplementsSetInfileStream]
            .setNextLocalInfileInputStream(inputstream)

          val finalQuery = appendTagsToQuery(conf, query)
          log.info(logEventNameTagger(s"Executing SQL:\n$finalQuery"))
          stmt.executeUpdate(finalQuery)
        } finally {
          stmt.close()
        }
      } finally {
        inputstream.close()
      }
    }
  }

  def createDataWriter(schema: StructType,
                       partitionId: Int,
                       attemptNumber: Int,
                       isReferenceTable: Boolean,
                       mode: SaveMode): DataWriter[Row] = {
    val avroSchema: Schema = if (conf.loadDataFormat == SinglestoreOptions.LoadDataFormat.Avro) {
      SchemaConverters.toAvroType(schema)
    } else {
      null
    }
    val query = createLoadDataQuery(schema, mode, avroSchema)

    val conn = SinglestoreConnectionPool.getConnection(if (isReferenceTable) {
      getDDLConnProperties(conf, isOnExecutor = true)
    } else {
      getDMLConnProperties(conf, isOnExecutor = true)
    })
    conn.setAutoCommit(false);

    val createDatabaseWriter: () => (OutputStream, Future[Long]) = () => {
      val (inputstream, outputstream) = createStreams()
      val writer = startStatementExecution(conn, query, inputstream)
      (outputstream, writer)
    }

    if (conf.loadDataFormat == SinglestoreOptions.LoadDataFormat.Avro) {
      new AvroDataWriter(avroSchema, createDatabaseWriter, conn, conf.insertBatchSize)
    } else {
      new LoadDataWriter(createDatabaseWriter, conn, conf.insertBatchSize)
    }
  }
}

class LoadDataWriter(createDatabaseWriter: () => (OutputStream, Future[Long]), conn: Connection, batchSize: Int)
    extends DataWriter[Row] {

  private var (outputstream, writeFuture) = createDatabaseWriter()
  private var rowsInBatch = 0

  override def write(row: Row): Unit = {
    if (rowsInBatch >= batchSize) {
      Try(outputstream.close())
      Await.result(writeFuture, Duration.Inf)
      val (newOutputStream, newWriteFuture) = createDatabaseWriter()
      outputstream = newOutputStream
      writeFuture = newWriteFuture
      rowsInBatch = 0
    }

    val rowLength = row.size
    for (i <- 0 until rowLength) {
      // We tried using off the shelf CSVWriter, but found it qualitatively slower.
      // The csv writer below has been benchmarked at 90 Mps going to a null output stream
      val value = row(i) match {
        case null => "\\N".getBytes(StandardCharsets.UTF_8)
        // NOTE: We special case booleans because SingleStore/MySQL's LOAD DATA
        // semantics only accept "1" as true in boolean/tinyint(1) columns
        case true               => "1".getBytes(StandardCharsets.UTF_8)
        case false              => "0".getBytes(StandardCharsets.UTF_8)
        case bytes: Array[Byte] => Base64.getEncoder.encode(bytes)
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

          valueString.getBytes(StandardCharsets.UTF_8)
        }
      }
      outputstream.write(value)
      outputstream.write(if (i < rowLength - 1) '\t' else '\n')
    }

    rowsInBatch += 1
  }

  override def commit(): WriterCommitMessage = {
    Try(outputstream.close())
    Await.result(writeFuture, Duration.Inf)
    conn.commit()
    new WriteSuccess
  }

  override def abort(writerException: Exception): Unit = {
    if (!conn.isClosed) {
      conn.abort(ExecutionContext.global)
    }
    Try(outputstream.close())
    try {
      Await.result(writeFuture, Duration.Inf)
    } catch {
      case readerException: Exception => {
        // if we got pipe closed error from the thread that writes data to stream
        // then the error actual occurred in the thread that ran the query
        // and we need to return the actual error
        if (writerException
              .isInstanceOf[IOException] && writerException.getMessage.contains("Pipe closed")) {
          throw readerException
        }
      }
    }
  }
}

class AvroDataWriter(avroSchema: Schema,
                     createDatabaseWriter: () => (OutputStream, Future[Long]),
                     conn: Connection,
                     batchSize: Int)
    extends DataWriter[Row] {

  private var (outputstream, writeFuture) = createDatabaseWriter()
  private var rowsInBatch = 0
  private var encoder     = EncoderFactory.get().binaryEncoder(outputstream, null)
  
  val datumWriter = new GenericDatumWriter[GenericRecord](avroSchema)
  val record      = new GenericData.Record(avroSchema)
  
  val conversionFunctions: Any => Any = {
    case d: java.math.BigDecimal =>
      d.toString
    case s: Short =>
      s.toInt
    case bytes: Array[Byte] =>
      ByteBuffer.wrap(bytes)
    case b: Byte =>
      b.toInt
    case num => num
  }

  override def write(row: Row): Unit = {
    if (rowsInBatch >= batchSize) {
      encoder.flush()
      Try(outputstream.close())
      Await.result(writeFuture, Duration.Inf)
      val (newOutputStream, newWriteFuture) = createDatabaseWriter()
      outputstream = newOutputStream
      writeFuture = newWriteFuture
      encoder = EncoderFactory.get().binaryEncoder(outputstream, null)
      rowsInBatch = 0
    }

    val rowLength = row.size
    for (i <- 0 until rowLength) {
      record.put(i, conversionFunctions(row(i)))
    }
    datumWriter.write(record, encoder)

    rowsInBatch += 1
  }

  override def commit(): WriterCommitMessage = {
    encoder.flush()
    Try(outputstream.close())
    Await.result(writeFuture, Duration.Inf)
    conn.commit()
    new WriteSuccess
  }

  override def abort(writerException: Exception): Unit = {
    if (!conn.isClosed) {
      conn.abort(ExecutionContext.global)
    }
    Try(outputstream.close())
    try {
      Await.result(writeFuture, Duration.Inf)
    } catch {
      case readerException: Exception => {
        // if we got pipe closed error from the thread that writes data to stream
        // then the error actual occurred in the thread that ran the query
        // and we need to return the actual error
        if (writerException
              .isInstanceOf[IOException] && writerException.getMessage.contains("Pipe closed")) {
          throw readerException
        }
      }
    }
  }
}
