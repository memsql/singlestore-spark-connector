package com.memsql.spark.connector

import com.memsql.spark.context.MemSQLContext

import scala.util.Random

import java.sql.{Connection, PreparedStatement, Types, Statement}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import org.apache.spark.{SparkException, Logging}

import java.io._

import java.util.zip.GZIPOutputStream
import java.io.{PipedOutputStream, PipedInputStream}
import net.jpountz.lz4._
import com.memsql.spark.connector.rdd.MemSQLRDD

class MemSQLException extends Exception
class NoMemSQLNodesAvailableException(message: String) extends MemSQLException
class SaveToMemSQLException(val exception: SparkException, val count: Long) extends MemSQLException

object RDDFunctions {
  val DEFAULT_UPSERT_BATCH_SIZE = 10000
  val LZ4_BLOCK_SIZE = 1024 * 1024 // bytes
}

object OnDupKeyBehavior extends Enumeration {
  type OnDupKeyBehavior = Value
  val Replace, Ignore, Update = Value
}
import com.memsql.spark.connector.OnDupKeyBehavior._

class RDDFunctions(rdd: RDD[Row]) extends Serializable with Logging {
  /**
   * Saves an [[org.apache.spark.rdd.RDD]]'s contents to a MemSQL database. The elements should
   * consist of [[org.apache.spark.sql.Row]]s of objects; each row should be the same length, and
   * the table specified in tableName should have as many columns as the arrays
   * have elements.
   *
   * If dbHost, dbPort, user and password are not specified, the [[com.memsql.spark.context.MemSQLContext]] will determine
   * where each partition's data is sent. If the Spark executors are colocated with writable MemSQL nodes, then each Spark
   * partition will insert into a randomly chosen colocated writable MemSQL node. If the Spark executors are not colocated
   * with writable MemSQL nodes, Spark partitions will insert writable MemSQL nodes round robin.
   *
   * @param dbName The name of the database.
   * @param tableName The name of the table.
   * @param onDuplicateKeyBehavior How to handle duplicate key errors when inserting rows. If this is [[OnDupKeyBehavior.Replace]],
   *                               we will replace existing rows with the ones in rdd. If this is [[OnDupKeyBehavior.Ignore]],
   *                               we will leave existing rows as they are. If this is Update, we will use the SQL code
   *                               in onDuplicateKeySql. If this is None, we will throw an error if there are any duplicate key errors.
   * @param onDuplicateKeySql Optional SQL to include in the "ON DUPLICATE KEY UPDATE" clause of the INSERT queries we generate.
   *                          If this is a non-empty string, onDuplicateKeyBehavior must be [[OnDupKeyBehavior.Update]].
   * @param upsertBatchSize How many rows to insert per INSERT query.  Has no effect if onDuplicateKeySql is not specified.
   * @param useKeylessShardedOptimization If set, data is loaded directly into leaf partitions. Can increase performance
   *                                      at the expense of higher variance sharding.
   * @param forceInsert If set, always use INSERT queries, not LOAD DATA, when
   *                    inserting data into MemSQL.
   * @return The number of rows inserted into MemSQL.
   */
  def saveToMemSQL(
    dbName: String,
    tableName: String,
    dbHost: String,
    dbPort: Int,
    user: String,
    password: String,
    onDuplicateKeyBehavior: Option[OnDupKeyBehavior] = None,
    onDuplicateKeySql: String = "",
    upsertBatchSize: Int = RDDFunctions.DEFAULT_UPSERT_BATCH_SIZE,
    useKeylessShardedOptimization: Boolean = false,
    forceInsert: Boolean = false): Long = {

    if (!onDuplicateKeySql.isEmpty && onDuplicateKeyBehavior != Some(OnDupKeyBehavior.Update)) {
      throw new IllegalArgumentException("If onDuplicateKeySql is set, then onDuplicateKeyBehavior must be set to Update")
    }
    if (onDuplicateKeyBehavior == Some(OnDupKeyBehavior.Update) && onDuplicateKeySql.isEmpty) {
      throw new IllegalArgumentException("If onDuplicateKeyBehavior is set to Update, then onDuplicateKeySql must be set")
    }

    var compression = "gzip"

    var availableNodes = MemSQLContext.getMemSQLNodesAvailableForIngest(dbHost, dbPort, user, password)
                                      .map { node => (node.host, node.port, dbName) }

    if (useKeylessShardedOptimization) {
      var wrapper: MemSQLConnectionWrapper = null
      var conn: Connection = null
      var stmt: Statement = null
      try {
        val randomIndex = Random.nextInt(availableNodes.size)
        wrapper =
          MemSQLConnectionPoolMap(
            availableNodes(randomIndex)._1, availableNodes(randomIndex)._2, user, password, "information_schema")
        conn = wrapper.conn
        stmt = conn.createStatement

        // NOTE: when a leaf is not online, its partitions will have Host, Port and Role set to NULL
        availableNodes = MemSQLRDD.resultSetToIterator(stmt.executeQuery("SHOW PARTITIONS FROM " + dbName))
                        .filter { row =>
                          val role = row.getString("Role")
                          role != null && role.equals("Master")
                        }.map(r => (r.getString("Host"), r.getInt("Port"), dbName + "_" + r.getString("Ordinal")))
                        .toList

        if (availableNodes.isEmpty) {
          throw new NoMemSQLNodesAvailableException("No MemSQL nodes are available for ingest. Are the MemSQL leaves online?")
        }
      } finally {
        if (stmt != null && !stmt.isClosed()) {
          stmt.close()
        }
        if (conn != null && !conn.isClosed()) {
          MemSQLConnectionPoolMap.returnConnection(wrapper)
        }
      }
    }

    val randomIndex = Random.nextInt(availableNodes.size)
    val numRowsAccumulator = rdd.sparkContext.accumulator[Long](0, "saveToMemSQL accumulator")
    try {
      rdd.foreachPartition{ part =>
        val node = chooseMemSQLTarget(availableNodes, randomIndex)
        if (node.isColocated) {
          compression = "tsv"
        }

        var numRowsAffected = 0
        if (onDuplicateKeySql.isEmpty && !forceInsert) {
          numRowsAffected = loadPartitionInMemSQL(
            node.targetHost, node.targetPort, user, password, node.targetDb, tableName,
            onDuplicateKeyBehavior, part, compression=compression)
        } else { // LOAD DATA ... ON DUPLICATE KEY REPLACE is not currently supported by memsql, so we still use insert in this case
          numRowsAffected = insertPartitionInMemSQL(
            node.targetHost, node.targetPort, user, password, node.targetDb, tableName, onDuplicateKeySql,
            upsertBatchSize, onDuplicateKeyBehavior, part)
        }
        numRowsAccumulator += numRowsAffected
      }
    } catch {
      case e: SparkException => throw new SaveToMemSQLException(e, numRowsAccumulator.value)
    }
    numRowsAccumulator.value
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
  private def chooseMemSQLTarget(availableNodes: List[(String, Int, String)], randomIndex: Int) : MemSQLTarget = {
    val hostname = TaskContext.get.taskMetrics.hostname
    val id = TaskContext.get.partitionId
    var myAvailableNodes = availableNodes.filter(_._1.equals(hostname))
    var ix = 0
    var isColocated = false
    if (myAvailableNodes.size == 0) { // there is no MemSQL node available for colocation
      myAvailableNodes = availableNodes
      ix = (randomIndex + TaskContext.get.partitionId) % myAvailableNodes.size
    } else { // there is at least one MemSQL node available for colocation
      ix = Random.nextInt(myAvailableNodes.size)
      isColocated = true
    }
    val node = myAvailableNodes(ix)
    MemSQLTarget(id, hostname, node._1, node._2, node._3, isColocated)
  }

  /*
   * A debugging utility.
   * Returns a List of MemSQLTarget structs, one for each Spark partition.
   * Represents one possibility for the Spark -> MemSQL mapping created by calling saveToMemSQL.
   */
  def saveToMemSQLDryRun(memSQLContext: MemSQLContext): List[MemSQLTarget] = {
    val availableNodes = memSQLContext.getMemSQLNodesAvailableForIngest().map(node => (node.host, node.port, null: String))
    val randomIndex = Random.nextInt(availableNodes.size)
    rdd.mapPartitions{ part =>
      List(chooseMemSQLTarget(availableNodes, randomIndex)).toIterator
    }.collect.toList
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
                                       onDuplicateKeyBehavior: Option[OnDupKeyBehavior] = None,
                                       iter: Iterator[Row]): Int = {
    var wrapper: MemSQLConnectionWrapper = null
    var conn: Connection = null
    var stmt: PreparedStatement = null
    var numOutputColumns = -1
    var numOutputRows = -1

    var numRowsAffected = 0

    try {
      wrapper = MemSQLConnectionPoolMap(dbHost, dbPort, user, password, dbName)
      conn = wrapper.conn
      conn.setAutoCommit(false)
      val groupedPartitionContents = iter.grouped(upsertBatchSize)
      for (group <- groupedPartitionContents) {
        val rowGroup = group.toList
        if (rowGroup.isEmpty) {
          return 0 // scalastyle:ignore
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
          onDuplicateKeyBehavior match {
            case Some(OnDupKeyBehavior.Replace) => sql.append("REPLACE ")
            case Some(OnDupKeyBehavior.Ignore) => sql.append("INSERT IGNORE ")
            case Some(OnDupKeyBehavior.Update) => sql.append("INSERT ")
            case _ => sql.append("INSERT ")
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
            if (x == null) {
              stmt.setNull(i, Types.NULL)
            } else {
              stmt.setObject(i, x)
            }
            i = i + 1
          }
        }
        numRowsAffected = stmt.executeUpdate()
      }
      conn.commit()
      numRowsAffected
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
          MemSQLConnectionPoolMap.returnConnection(wrapper)
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
                                    onDuplicateKeyBehavior: Option[OnDupKeyBehavior] = None,
                                    iter: Iterator[Row],
                                    compression:String = "gzip"): Int = {

    val basestream = new PipedOutputStream
    val input = new PipedInputStream(basestream)

    var outstream: OutputStream = basestream
    var ext = "tsv"

    var numRowsAffected = 0

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
        outstream = new LZ4BlockOutputStream(outstream, RDDFunctions.LZ4_BLOCK_SIZE, LZ4Factory.fastestInstance.fastCompressor)
        assert(false, "we don't quite have lz4 working yet")
      }
      case default => { }
    }

    val onDupKeyStr = onDuplicateKeyBehavior match {
      case Some(OnDupKeyBehavior.Replace) => "REPLACE "
      case Some(OnDupKeyBehavior.Ignore) => "IGNORE "
      case _ => ""
    }
    val q = "LOAD DATA LOCAL INFILE '###." + ext + "' " + onDupKeyStr + "INTO TABLE " + tableName

    @volatile var writerException: Exception = null

    new Thread(new Runnable {
      override def run(): Unit = {
        try {
          for (row <- iter) {
            for (i <- 0 until row.size) {
              // We tried using off the shelf CSVWriter, but found it qualitatively slower.
              // The csv writer below has been benchmarked at 90 Mps going to a null output stream
              //
              val elt = row(i) match {
                case null => "\\N"
                // NOTE: We special case booleans because MemSQL/MySQL's LOAD DATA semantics only accept "1" as true
                // in boolean/tinyint(1) columns
                case true => "1"
                case false => "0"
                case default => {
                  var eltString = row(i).toString
                  if (eltString.indexOf('\\') != -1) { eltString = eltString.replace("\\","\\\\") }
                  if (eltString.indexOf('\n') != -1) { eltString = eltString.replace("\n","\\n")  }
                  if (eltString.indexOf('\t') != -1) { eltString = eltString.replace("\t","\\t")  }
                  eltString
                }
              }
              outstream.write(elt.getBytes)
              outstream.write(if (i== row.size - 1) '\n' else '\t')
            }
          }
        } catch {
          case e: Exception => writerException = e
        } finally {
          outstream.close()
        }
      }
    }).start()
    var wrapper: MemSQLConnectionWrapper = null
    var conn: Connection = null
    var stmt: com.mysql.jdbc.Statement = null
    try {
      wrapper = MemSQLConnectionPoolMap(dbHost, dbPort, user, password, dbName)
      conn = wrapper.conn
      stmt = conn.createStatement.asInstanceOf[com.mysql.jdbc.Statement]
      stmt.setLocalInfileInputStream(input)
      numRowsAffected = stmt.executeUpdate(q)
      if (writerException != null) {
        throw writerException
      }
    } finally {
      if (stmt != null && !stmt.isClosed()) {
        stmt.close()
      }
      if (null != conn && !conn.isClosed()) {
        MemSQLConnectionPoolMap.returnConnection(wrapper)
      }
    }
    numRowsAffected
  }
}
