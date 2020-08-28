package com.memsql.spark.v2

import java.io.{InputStream, OutputStream, PipedInputStream, PipedOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.util.Base64
import java.util.zip.GZIPOutputStream

import com.memsql.spark.MemsqlOptions.CompressionType
import com.memsql.spark._
import com.memsql.spark.vendor.apache.SchemaConverters
import net.jpountz.lz4.LZ4FrameOutputStream
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types.{BinaryType, StructType}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class MemsqlLoadDataWriterFactory(schema: StructType,
                                  partitionId: Int,
                                  attemptNumber: Int,
                                  isReferenceTable: Boolean,
                                  mode: SaveMode,
                                  table: TableIdentifier,
                                  conf: MemsqlOptions)
    extends DataWriterFactory
    with LazyLogging {

  final val BUFFER_SIZE = 524288

  type ImplementsSetInfileStream = {
    def setLocalInfileInputStream(input: InputStream)
  }

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
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

    def tempColName(colName: String) = s"@${colName}_tmp"

    val columnNames = schema.map(s =>
      if (s.dataType == BinaryType) {
        tempColName(s.name)
      } else {
        MemsqlDialect.quoteIdentifier(s.name)
    })

    val loadDataFormat = conf.loadDataFormat
    val querySetPart =
      if (loadDataFormat == MemsqlOptions.LoadDataFormat.Avro) ""
      else {
        val binaryColumns = schema.filter(_.dataType == BinaryType)
        if (binaryColumns.isEmpty) {
          ""
        } else {
          val operations = binaryColumns
            .map(s =>
              s"${MemsqlDialect.quoteIdentifier(s.name)} = FROM_BASE64(${tempColName(s.name)})")
          s"SET ${operations.mkString(" ")}"
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
    var avroSchema: Schema = null
    val queryPrefix        = s"LOAD DATA LOCAL INFILE '###.$ext'"
    val queryEnding = if (loadDataFormat == MemsqlOptions.LoadDataFormat.Avro) {
      avroSchema = SchemaConverters.toAvroType(schema)
      val nullableSchemas = for ((field, index) <- schema.fields.zipWithIndex)
        yield
          AvroSchemaHelper.resolveNullableType(avroSchema.getFields.get(index).schema(),
                                               field.nullable)
      val avroSchemaParts = for ((field, index) <- schema.fields.zipWithIndex) yield {
        val avroSchemaMapping =
          s"${MemsqlDialect.quoteIdentifier(field.name)} <- %::${MemsqlDialect.quoteIdentifier(field.name)}"
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

    val conn = JdbcUtils.createConnectionFactory(
      if (isReferenceTable) {
        JdbcHelpers.getDDLJDBCOptions(conf)
      } else {
        JdbcHelpers.getDMLJDBCOptions(conf)
      }
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
    if (loadDataFormat == MemsqlOptions.LoadDataFormat.Avro) {
      new MemsqlAvroDataWriter(avroSchema, outputstream, writer, conn, schema)
    } else {
      new MemsqlCSVDataWriter(outputstream, writer, conn, schema)
    }
  }
}

class MemsqlCSVDataWriter(outputstream: OutputStream,
                          writeFuture: Future[Long],
                          conn: Connection,
                          schema: StructType)
    extends DataWriter[InternalRow] {

  override def write(row: InternalRow): Unit = {
    val rowLength = row.toSeq(schema).size
    for ((col, i) <- row.toSeq(schema).zipWithIndex) {

      // We tried using off the shelf CSVWriter, but found it qualitatively slower.
      // The csv writer below has been benchmarked at 90 Mps going to a null output stream
      val value = col match {
        case null => "\\N".getBytes(StandardCharsets.UTF_8)
        // NOTE: We special case booleans because MemSQL/MySQL's LOAD DATA
        // semantics only accept "1" as true in boolean/tinyint(1) columns
        case true               => "1".getBytes(StandardCharsets.UTF_8)
        case false              => "0".getBytes(StandardCharsets.UTF_8)
        case bytes: Array[Byte] => Base64.getEncoder.encode(bytes)
        case rawValue =>
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
      outputstream.write(value)
      outputstream.write(if (i < rowLength - 1) '\t' else '\n')
    }
  }

  override def commit(): WriterCommitMessage = {
    outputstream.close()
    Await.result(writeFuture, Duration.Inf)
    new WriteSuccess
  }

  override def abort(): Unit = {
    conn.abort(ExecutionContext.global)
    outputstream.close()
    Await.ready(writeFuture, Duration.Inf)
  }

  override def close(): Unit = {
    outputstream.close()
    conn.close()
  }
}

class MemsqlAvroDataWriter(avroSchema: Schema,
                           outputstream: OutputStream,
                           writeFuture: Future[Long],
                           conn: Connection,
                           schema: StructType)
    extends DataWriter[InternalRow] {

  val datumWriter = new GenericDatumWriter[GenericRecord](avroSchema)
  val encoder     = EncoderFactory.get().binaryEncoder(outputstream, null)
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

  override def write(row: InternalRow): Unit = {
    val rowLength = row.toSeq(schema).size
    for (i <- 0 until rowLength) {
      record.put(i, conversionFunctions(row.toSeq(schema)(i)))
    }
    datumWriter.write(record, encoder)
  }

  override def commit(): WriterCommitMessage = {
    encoder.flush()
    outputstream.close()
    Await.result(writeFuture, Duration.Inf)
    new WriteSuccess
  }

  override def abort(): Unit = {
    conn.abort(ExecutionContext.global)
    outputstream.close()
    Await.ready(writeFuture, Duration.Inf)
  }

  override def close(): Unit = {
    outputstream.close()
    conn.close()
  }
}
