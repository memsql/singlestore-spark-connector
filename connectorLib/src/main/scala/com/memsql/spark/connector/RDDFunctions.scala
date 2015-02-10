package com.memsql.spark.connector

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class RDDFunctions[T : ClassTag](rdd: RDD[Array[T]]) extends Serializable with Logging {

  /**
   * Saves an RDD's contents to a MemSQL database.  The RDD's elements should
   * consist of arrays of objects; each array should be the same length, and
   * the table specified in tableName should have as many columns as the arrays
   * have elements.
   *
   * @param dbHost the host to connect to for the master aggregator of the
   * MemSQL cluster.
   * @param dbPort the port to connect to for the master aggregator of the
   * MemSQL cluster.
   * @param user the username to use when connecting to the databases in the
   *   MemSQL cluster.  All the nodes in the cluster should use the same user.
   * @param password the password to use when connecting to the databases in the
   *   MemSQL cluster.  All the nodes in the cluster should use the same
   *   password.
   * @param dbName the name of the database we're working in.
   * @param tableName the name of the table we're saving the data in.
   * @param onDuplicateKeySql Optional SQL to include in the
   *   "ON DUPLICATE KEY UPDATE" clause of the INSERT queries we generate.
   * @param insertBatchSize How many rows to insert per INSERT query.
   */
  def saveToMemsql(
    dbHost: String,
    dbPort: Int,
    user: String,
    password: String,
    dbName: String,
    tableName: String,
    onDuplicateKeySql: String = "",
    insertBatchSize: Int = 10000) {
    rdd.foreachPartition(
      insertPartitionInMemsql(
        dbHost, dbPort, user, password, dbName, tableName, onDuplicateKeySql,
        insertBatchSize, _: Iterator[Array[T]]))
  }

  private def insertPartitionInMemsql(
    dbHost: String,
    dbPort: Int,
    user: String,
    password: String,
    dbName: String,
    tableName: String,
    onDuplicateKeySql: String,
    insertBatchSize: Int,
    iter: Iterator[Array[T]]) {
    var conn: Connection = null
    try {
      conn = getMemSQLConnection(dbHost, dbPort, user, password, dbName)
      conn.setAutoCommit(false)
      val groupedPartitionContents = iter.grouped(insertBatchSize)
      for (group <- groupedPartitionContents) {
        val rowGroup = group.toArray
        if (rowGroup.isEmpty) {
          return
        }
        val numOutputColumns = rowGroup(0).length
        var sql = "INSERT INTO " + tableName + " VALUES "
        for (i <- 0 until rowGroup.length - 1) {
          sql = sql + "(" + "?," * (numOutputColumns - 1) + "?),"
        }
        sql = sql + "(" + "?," * (numOutputColumns - 1) + "?)"
        if (!onDuplicateKeySql.isEmpty) {
          sql = sql + " ON DUPLICATE KEY UPDATE " + onDuplicateKeySql
        }
        val stmt = conn.prepareStatement(sql)
        var i = 1
        for (row <- rowGroup) {
          if (row.length != numOutputColumns) {
            throw new SparkException("Unequal row lengths in parent RDD")
          }
          for (x <- row) {
            stmt.setObject(i, x)
            i = i + 1
          }
        }
        val numRowsAffected = stmt.executeUpdate()
        try {
          if (!stmt.isClosed()) {
            stmt.close()
          }
        } catch {
          case e: Exception => logWarning("Exception closing statement", e)
        }
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
        if (null != conn && ! conn.isClosed()) {
          conn.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }
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
