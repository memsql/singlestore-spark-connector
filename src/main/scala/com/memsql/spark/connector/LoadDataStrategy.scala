package com.memsql.spark.connector

import java.io.{PipedInputStream, PipedOutputStream}
import java.util.zip.GZIPOutputStream

import com.memsql.spark.connector.sql.{QueryFragments, QueryFragment}
import com.memsql.spark.connector.util.MemSQLConnectionInfo
import com.mysql.jdbc.Statement
import org.apache.commons.dbcp2.DelegatingStatement
import org.apache.spark.sql.{Row, SaveMode}
import com.memsql.spark.connector.util.JDBCImplicits._

import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global

case class LoadDataStrategy(tableFragment: QueryFragment,
                            conf: SaveToMemSQLConf
                           ) extends IngestStrategy {

  override def loadPartition(connInfo: MemSQLConnectionInfo, partition: Iterator[Row]): Long = {
    val BUFFER_SIZE = 524288

    val basestream = new PipedOutputStream
    val input = new PipedInputStream(basestream, BUFFER_SIZE)

    val compressionType =
      if (connInfo.isColocated) { CompressionType.Skip }
      else { conf.loadDataCompression }

    val (ext, outstream) = compressionType match {
      case CompressionType.GZip => {
        // With gzip default 1 we get a 50% improvement in bandwidth
        // (up to 16 Mps) over gzip default 6 on customer workload.
        //
        ("gz", new GZIPOutputStream(basestream) {{ `def`.setLevel(1) }})
      }
      case _ => ("tsv", basestream)
    }

    val onDupKeyStr = conf.saveMode match {
      case SaveMode.Append => "IGNORE"
      case SaveMode.Ignore => "IGNORE"
      case SaveMode.Overwrite => "REPLACE"
      case _ => ""
    }

    val query: QueryFragment = QueryFragments.loadDataQuery(tableFragment, ext, onDupKeyStr)

    @volatile var writerException: Exception = null

    val writeToMemSQL = Future[Long] {
      MemSQLConnectionPool.withConnection(connInfo) { conn =>
        conn.withStatement(stmt => {
          val dbcpStmt = stmt.asInstanceOf[DelegatingStatement]
          val mysqlStmt = dbcpStmt.getInnermostDelegate.asInstanceOf[Statement]
          mysqlStmt.setLocalInfileInputStream(input)

          val numRowsAffected = stmt.executeUpdate(query.sql.toString)

          numRowsAffected
        })
      }
    }

    try {
      for (row <- partition) {
        for (i <- 0 until row.size) {
          // We tried using off the shelf CSVWriter, but found it qualitatively slower.
          // The csv writer below has been benchmarked at 90 Mps going to a null output stream
          //
          val value = row(i) match {
            case null => "\\N"
            // NOTE: We special case booleans because MemSQL/MySQL's LOAD DATA
            // semantics only accept "1" as true in boolean/tinyint(1) columns
            case true => "1"
            case false => "0"
            case default => {
              var valueString = row(i).toString
              if (valueString.indexOf('\\') != -1) { valueString = valueString.replace("\\","\\\\") }
              if (valueString.indexOf('\n') != -1) { valueString = valueString.replace("\n","\\n")  }
              if (valueString.indexOf('\t') != -1) { valueString = valueString.replace("\t","\\t")  }
              valueString
            }
          }
          outstream.write(value.getBytes)
          outstream.write(if (i== row.size - 1) '\n' else '\t')
        }
      }
    } finally {
      outstream.close()
    }

    Await.result(writeToMemSQL, scala.concurrent.duration.Duration.Inf )
  }
}
