package com.memsql.spark.connector

import scala.util.Random

import java.sql.{Connection, DriverManager, PreparedStatement, Types, Statement}
// import org.apache.spark.util.Utils TODO: this object is private

import org.apache.spark.{Logging, SparkException, TaskContext}
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
import com.memsql.spark.context.MemSQLSparkContext
import com.memsql.spark.connector.rdd.MemSQLRDD

class RDDFunctions(rdd: RDD[Row]) extends Serializable with Logging {


  /**
   * Saves an RDD's contents to a MemSQL database.  The RDD's elements should
   * consist of arrays of objects; each array should be the same length, and
   * the table specified in tableName should have as many columns as the arrays
   * have elements.
   *
   * If dbHost, dbPort, user and password are not specified,
   * the MemSQLSparkContext will determine where each partition's data is sent.
   * Otherwise, all partitions will load into the node specified by MemSQLSparkContext.
   *
   * If MemSQLSparkContext is used to determine a partition's destination,
   * and if the Spark executors are colocated with writable MemSQL nodes,
   * then each Spark partition will insert into a randomly chosen colocated writable MemSQL node.
   * If MemSQLSparkContext is used to determine a partitions's destination
   * and the Spark executors are not colocated with writable MemSQL nodes,
   * Spark partitions will insert writable MemSQL nodes round robin.
   *
   * @param dbName the name of the database we're working in.
   * @param tableName the name of the table we're saving the data in.
   * @param onDuplicateKeySql Optional SQL to include in the
   *                          "ON DUPLICATE KEY UPDATE" clause of the INSERT queries we generate.
   * @param upsertBatchSize How many rows to insert per INSERT query.  Has no effect if onDuplicateKeySql is not specified.
   * @param useKeylessShardedOptimization if set, data is loaded directly into leaf partitions.  Can increased performance at the expense of higher variance sharding.
   */
  def saveToMemSQL(
    dbName: String,
    tableName: String,
    dbHost: String = null,
    dbPort: Int = -1,
    user: String = null,
    password: String = null,
    onDuplicateKeySql: String = "",
    useInsertIgnore: Boolean = false,
    upsertBatchSize: Int = 10000,
    useKeylessShardedOptimization: Boolean = false) {

    var theUser = user
    var thePassword = password
    var compression = "gzip"
    var availableNodes: Array[(String,Int,String)] = Array((dbHost,dbPort,dbName))
    if (dbHost == null || dbPort == -1 || user == null || password == null) {
      rdd.sparkContext match {
        case _: MemSQLSparkContext => {
          val msc = rdd.sparkContext.asInstanceOf[MemSQLSparkContext]
          theUser = msc.GetUserName
          thePassword = msc.GetPassword
          availableNodes = msc.GetMemSQLNodesAvailableForIngest.map(node => (node._1,node._2,dbName))
        }
        case _ => {
          throw new SparkException("saveToMemSQL requires intializing Spark with MemSQLSparkContext or explicitly setting dbName, dbHost, user and password")
        }
      }
    }
    if (useKeylessShardedOptimization) {
      var conn: Connection = null
      var stmt: Statement = null
      try {
        val randomIndex = Random.nextInt(availableNodes.size)
        val dbAddress = "jdbc:mysql://" + availableNodes(randomIndex)._1 + ":" + availableNodes(randomIndex)._2
        conn = DriverManager.getConnection(dbAddress, theUser, thePassword)
        stmt = conn.createStatement
        availableNodes = MemSQLRDD.resultSetToIterator(stmt.executeQuery("SHOW PARTITIONS FROM " + dbName))
                        .filter(_.getString("Role").equals("Master"))
                        .map(r => (r.getString("Host"), r.getInt("Port"), dbName + "_" + r.getString("Ordinal")))
                        .toArray
      } finally {
        if (stmt != null && !stmt.isClosed()) {
          stmt.close()
        }
        if (conn != null && !conn.isClosed()) {
          conn.close()
        }
      }
    }

    val randomIndex = Random.nextInt(availableNodes.size)
    rdd.foreachPartition{ part =>
      val node = chooseMemSQLTarget(availableNodes, randomIndex)
      if (node.isColocated) {
        compression = "tsv"
      }

      if (onDuplicateKeySql.isEmpty) {
        loadPartitionInMemSQL(
          node.targetHost, node.targetPort, theUser, thePassword, node.targetDb, tableName,
          useInsertIgnore, part, compression=compression)
      } else { // LOAD DATA ... ON DUPLICATE KEY REPLACE is not currently supported by memsql, so we still use insert in this case
        insertPartitionInMemSQL(
          node.targetHost, node.targetPort, theUser, thePassword, node.targetDb, tableName, onDuplicateKeySql,
          upsertBatchSize, useInsertIgnore, part)
      }
    }
  }

  /*
   * A struct enclosing the data required for loading a Spark partition into a MemSQL node
   */
  case class MemSQLTarget(partitionIndex: Int,
                          myHostName:  String,
                          targetHost:  String,
                          targetPort:  Int,
                          targetDb:    String,
                          isColocated: Boolean)

  /*
   * From a list of possibilities, chooses a MemSQLTarget.
   * Must be called from an executor.
   */
  private def chooseMemSQLTarget(availableNodes: Array[(String, Int, String)], randomIndex: Int) : MemSQLTarget = {
    val hostname = TaskContext.get.taskMetrics.hostname
    val id = TaskContext.get.partitionId
    var myAvailableNodes = availableNodes.filter(_._1.equals(hostname))
    var ix = 0
    var isColocated = false
    if (myAvailableNodes.size == 0) { // there is no MemSQL node available for colocation
      myAvailableNodes = availableNodes
      ix = (randomIndex + TaskContext.get.partitionId) % myAvailableNodes.size
    } else { // there is at least one MemSQL node avaiable for colocation
      ix = Random.nextInt(myAvailableNodes.size)
      isColocated = true
    }
    val node = myAvailableNodes(ix)
    MemSQLTarget(id, hostname, node._1, node._2, node._3, isColocated)
  }

  /*
   * A debugging utility.
   * Returns an Array of MemSQLTarget structs, one for each Spark partition.
   * Represents one possibility for the Spark -> MemSQL mapping created by calling saveToMemSQL.
   */
  def saveToMemSQLDryRun : Array[MemSQLTarget] = {
    val msc = rdd.sparkContext.asInstanceOf[MemSQLSparkContext]
    val availableNodes = msc.GetMemSQLNodesAvailableForIngest.map(node => (node._1,node._2,null: String))
    val randomIndex = Random.nextInt(availableNodes.size)
    rdd.mapPartitions{ part =>
      Array(chooseMemSQLTarget(availableNodes, randomIndex)).toIterator
    }.collect
  }

  private def insertPartitionInMemSQL(
                                       dbHost: String,
                                       dbPort: Int,
                                       user: String,
                                       password: String,
                                       dbName: String,
                                       tableName: String,
                                       onDuplicateKeySql: String,
                                       upsertBatchSize: Int,
                                       useInsertIgnore: Boolean,
                                       iter: Iterator[Row]) {
    var conn: Connection = null
    var stmt: PreparedStatement = null
    var numOutputColumns = -1
    var numOutputRows = -1

    try {
      conn = getMemSQLConnection(dbHost, dbPort, user, password, dbName)
      conn.setAutoCommit(false)
      val groupedPartitionContents = iter.grouped(upsertBatchSize)
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
        // with gzip default 1 we get a 50% improvement in bandwidth (up to 16 Mps) over gzip default 6 on customer workload
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
      case default => { }
    }

    val q = "LOAD DATA LOCAL INFILE '###." + ext + "' " + (if (useInsertIgnore) "IGNORE " else "") + "INTO TABLE " + tableName

    new Thread(new Runnable {
      override def run(): Unit = {
        try {
          for (row <- iter) {
            for (i <- 0 until row.size) {
              // We tried using off the shelf CSVWriter, but found it qualitatively slower.
              // The csv writer below has been benchmarked at 90 Mps going to a null output stream
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
