package com.memsql.spark.connector

import java.sql.{Connection, DriverManager, PreparedStatement, Types}
// import org.apache.spark.util.Utils TODO: this object is private

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.reflect.ClassTag
import org.apache.spark.{SparkException, Logging}

import java.util.UUID
import java.io._
import java.lang.System

import org.apache.commons.io.output._
import java.util.zip.GZIPOutputStream
import java.io.{PipedOutputStream, PipedInputStream}
import net.jpountz.lz4._

class RDDFunctions(rdd: RDD[Row]) extends Serializable with Logging {

  /**
   * Saves an RDD's contents to a MemSQL database.  The RDD's elements should
   * consist of arrays of objects; each array should be the same length, and
   * the table specified in tableName should have as many columns as the arrays
   * have elements.
   *
   * @param dbHost the host to connect to for the master aggregator of the
   *               MemSQL cluster.
   * @param dbPort the port to connect to for the master aggregator of the
   *               MemSQL cluster.
   * @param user the username to use when connecting to the databases in the
   *             MemSQL cluster.  All the nodes in the cluster should use the same user.
   * @param password the password to use when connecting to the databases in the
   *                 MemSQL cluster.  All the nodes in the cluster should use the same
   *                 password.
   * @param dbName the name of the database we're working in.
   * @param tableName the name of the table we're saving the data in.
   * @param onDuplicateKeySql Optional SQL to include in the
   *                          "ON DUPLICATE KEY UPDATE" clause of the INSERT queries we generate.
   * @param insertBatchSize How many rows to insert per INSERT query.
   */
  def saveToMemSQL(
                    dbHost: String,
                    dbPort: Int,
                    user: String,
                    password: String,
                    dbName: String,
                    tableName: String,
                    onDuplicateKeySql: String = "",
                    useInsertIgnore: Boolean = false,
                    insertBatchSize: Int = 10000) {
    rdd.foreachPartition{
      if (onDuplicateKeySql.isEmpty) { // LOAD DATA ... ON DUPLICATE KEY REPLACE is not currently supported by memsql, so we still use insert in this case
        loadPartitionInMemSQL(
          dbHost, dbPort, user, password, dbName, tableName, 
          useInsertIgnore, _: Iterator[Row])
      } else {
        insertPartitionInMemSQL(
          dbHost, dbPort, user, password, dbName, tableName, onDuplicateKeySql, 
          insertBatchSize, useInsertIgnore, _: Iterator[Row])
      }
    }
  }

  private def insertPartitionInMemSQL(
                                       dbHost: String,
                                       dbPort: Int,
                                       user: String,
                                       password: String,
                                       dbName: String,
                                       tableName: String,
                                       onDuplicateKeySql: String,
                                       insertBatchSize: Int,
                                       useInsertIgnore: Boolean, 
                                       iter: Iterator[Row]) {
    var conn: Connection = null
    var stmt: PreparedStatement = null
    var numOutputColumns = -1
    var numOutputRows = -1

    try {
      conn = getMemSQLConnection(dbHost, dbPort, user, password, dbName)
      conn.setAutoCommit(false)
      val groupedPartitionContents = iter.grouped(insertBatchSize)
      for (group <- groupedPartitionContents) {
        val rowGroup = group.toArray
        if (rowGroup.isEmpty) {
          return
        }
        if (numOutputColumns != rowGroup(0).length || numOutputRows != rowGroup.length) {
          try {
            if (stmt != null && !stmt.isClosed()) {
              stmt.close()
            }
          } catch {
            case e: Exception => logWarning("Exception closing statement", e)
          }
          numOutputRows = rowGroup.length
          numOutputColumns = rowGroup(0).length
          val sql = new StringBuilder()
          sql.append("INSERT ")
          if (useInsertIgnore) {
            sql.append("IGNORE ")
          }
          sql.append("INTO ").append(tableName).append(" VALUES")
          for (rowId <- 0 until numOutputRows) {
            if (rowId > 0) {
              sql.append(",")
            }
            sql.append("(")
            for (columnId <- 0 until numOutputColumns) {
              if (columnId > 0) {
                sql.append(",")
              }
              sql.append("?")
            }
            sql.append(")")
          }
          if (!onDuplicateKeySql.isEmpty) {
            sql.append(" ON DUPLICATE KEY UPDATE ").append(onDuplicateKeySql)
          }
          stmt = conn.prepareStatement(sql.toString())
        }
        var i = 1
        for (row <- rowGroup) {
          if (row.length != numOutputColumns) {
            throw new SparkException("Unequal row lengths in parent RDD")
          }
          for (j <- 0 until row.size) {
            val x = row.get(j)
            if (x == null)
            {
              stmt.setNull(i, Types.NULL)
            }
            else
            {
              stmt.setObject(i, x)
            }
            i = i + 1
          }
        }
        val numRowsAffected = stmt.executeUpdate()
      }
      conn.commit()
    } catch {
      case e: Exception => {
        if (conn != null) {
          conn.rollback()
        }
        throw e
      }
    } finally {
      try {
        if (stmt != null && !stmt.isClosed()) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn && !conn.isClosed()) {
          conn.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }
  }
  private def loadPartitionInMemSQL(
                                    dbHost: String,
                                    dbPort: Int,
                                    user: String,
                                    password: String,
                                    dbName: String,
                                    tableName: String,
                                    useInsertIgnore: Boolean, 
                                    iter: Iterator[Row],
                                    compression:String = "gzip") {
      
    val basestream = new PipedOutputStream
    val input = new PipedInputStream(basestream)

    var outstream: OutputStream = basestream    
    var ext = "tsv"
    
    compression match {
      case "gzip" => {
        ext = "gz"
        // with gzip default 1 we get a 50% improvement in bandwith (up to 16 Mps) over gzip default 6 on customer workload
        //
        outstream = new GZIPOutputStream(basestream) {{ `def`.setLevel(1) }}
      }
      case "lz4" => {
        ext = "lz4"
        // blocksize for lz4 can be tween 16k (default) and 32 meg.
        // we set to 1 meg, and have no data to support that this is faster.
        //
        outstream = new LZ4BlockOutputStream(outstream, 1048576, LZ4Factory.fastestInstance.fastCompressor)
        assert(false, "we don't quite have lz4 working yet")
      }
      case "" => { }
    }

    val q = "LOAD DATA LOCAL INFILE '###." + ext + "' " + (if (useInsertIgnore) "IGNORE " else "") + "INTO TABLE " + tableName

    new Thread(new Runnable {
      override def run(): Unit = {
        try {
          for (row <- iter) {
            for (i <- 0 until row.size) {
              // We tried using off the shelf CSVWriter, but found it qualitatively slower.
              // The csv writer below has been benchmarked at 90 Mps goint to a null output stream
              //
              var elt = ""
              if (row(i) == null) {
                elt = "\\N" 
              } else {                
                elt = row(i).toString
                if (elt.indexOf('\\') != -1) { elt = elt.replace("\\","\\\\") }
                if (elt.indexOf('\n') != -1) { elt = elt.replace("\n","\\n")  }
                if (elt.indexOf('\t') != -1) { elt = elt.replace("\t","\\t")  }
              }
              outstream.write(elt.getBytes)
              outstream.write(if (i== row.size - 1) '\n' else '\t')
            }              
          }
        }
        finally {
          outstream.close()
        }
      }
    }).start()
    val conn = getMemSQLConnection(dbHost, dbPort, user, password, dbName)
    val stmt = conn.createStatement.asInstanceOf[com.mysql.jdbc.Statement]
    stmt.setLocalInfileInputStream(input)
    stmt.executeQuery(q)
    stmt.close()
    conn.close()    
  }  

  private def getMemSQLConnection(
                                   dbHost: String,
                                   dbPort: Int,
                                   user: String,
                                   password: String,
                                   dbName: String): Connection = {
    // Make sure the JDBC driver is on the classpath.
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val dbAddress = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName
    DriverManager.getConnection(dbAddress, user, password)
  }
}

// TODO: make this work without copying data.  
// The original version of RDDFunctions worked on an an RDD[Array[T]].
// The code above works both for RDD[Array[T]] and RDD[Row], but in a duck-typed c++-templates sense
// Row and Array[T] have no nontrivial common superclass (why not Seq[T]? import the implicit?), so I'm not sure how to do the code sharing in scala, 
// but I don't want to worry about it now, so I just copy everything
// 
class RDDFunctionsLegacy[T: ClassTag](rdd: RDD[Array[T]]) extends Serializable with Logging {
  def saveToMemSQL(
                    dbHost: String,
                    dbPort: Int,
                    user: String,
                    password: String,
                    dbName: String,
                    tableName: String,
                    onDuplicateKeySql: String = "",
                    useInsertIgnore: Boolean = false,
                    insertBatchSize: Int = 10000) {
    new RDDFunctions(rdd.map((r: Array[T]) => Row.fromSeq(Range(0,r.size).map(r(_))))) // TODO: this is idiomatically very wrong and probably copies the data TWICE
                     .saveToMemSQL(dbHost, dbPort, user, password, dbName, tableName, onDuplicateKeySql, useInsertIgnore, insertBatchSize)
  }
}
