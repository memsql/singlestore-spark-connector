package com.memsql.spark.connector.rdd

import java.sql.{Connection, ResultSet, Statement, PreparedStatement, Types}
import com.memsql.spark.connector.{MemSQLConnectionPool, MemSQLCluster}

import scala.reflect.ClassTag

import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import com.memsql.spark.connector.util.{MemSQLConnectionInfo, NextIterator}
import com.memsql.spark.connector.util.JDBCImplicits._

import scala.util.Try

class MemSQLRDDPartition(override val index: Int,
                         val connectionInfo: MemSQLConnectionInfo,
                         val query: Option[String]=None
                        ) extends Partition

case class ExplainRow(selectType: String, extra: String, query: String)

/**
  * An [[org.apache.spark.rdd.RDD]] that can read data from a MemSQL database based on a SQL query.
  *
  * If the given query supports it, this RDD will read data directly from the
  * MemSQL cluster's leaf nodes rather than from the master aggregator, which
  * typically results in much faster reads.  However, if the given query does
  * not support this (e.g. queries involving joins or GROUP BY operations), the
  * results will be returned in a single partition.
  *
  * @param cluster A connected MemSQLCluster instance.
  * @param sql The text of the query. Can be a prepared statement template,
  *    in which case parameters from sqlParams are substituted.
  * @param sqlParams The parameters of the query if sql is a template.
  * @param databaseName Optionally provide a database name for this RDD.
  *                     This is required for Partition Pushdown
  * @param mapRow A function from a ResultSet to a single row of the desired
  *   result type(s).  This should only call getInt, getString, etc; the RDD
  *   takes care of calling next.  The default maps a ResultSet to an array of
  *   Object.
  */
case class MemSQLRDD[T: ClassTag](@transient sc: SparkContext,
                                  cluster: MemSQLCluster,
                                  sql: String,
                                  sqlParams: Seq[Any] = Nil,
                                  databaseName: Option[String] = None,
                                  mapRow: (ResultSet) => T = MemSQLRDD.resultSetToObjectArray(_)
                                 ) extends RDD[T](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    // databaseName is required for partition pushdown
    if (databaseName.isEmpty) {
      Array[Partition](new MemSQLRDDPartition(0, cluster.getRandomAggregatorInfo))
    } else {
      cluster.withMasterConn { conn =>
        val explainSQL = "EXPLAIN EXTENDED " + sql
        val explainResult = conn.withPreparedStatement(explainSQL, stmt => {
          stmt.fillParams(sqlParams)
          val result = stmt.executeQuery
          if (result.hasColumn("Query")) {
            Some(
              result
                .toIterator
                .map(r => {
                  val selectType = r.getString("select_type")
                  val extra = r.getString("Extra")
                  val query = r.getString("Query")
                  ExplainRow(selectType, extra, query)
                })
                .toSeq
            )
          } else {
            None
          }
        })

        // Inspect the explain output to determine if we
        // can safely pushdown the query to the leaves.
        // Currently we only pushdown queries which have a
        // single SIMPLE query, and are a Simple Iterator on the agg.
        // TODO: Support pushing down more complex queries (like distributed joins)

        val partitionSQLTemplate = explainResult.flatMap(explainRows => {
          val isSimpleIterator = explainRows(0).extra == "memsql: Simple Iterator -> Network"
          val hasMultipleRows = explainRows.length > 1
          val noDResult = !explainRows.exists(_.selectType == "DRESULT")

          if (isSimpleIterator && hasMultipleRows && noDResult) {
            Some(explainRows(1).query)
          } else {
            None
          }
        })

        if (partitionSQLTemplate.isEmpty) {
          Array[Partition](new MemSQLRDDPartition(0, cluster.getRandomAggregatorInfo))
        } else {
          val template = partitionSQLTemplate.get
          val dbName = databaseName.get

          conn.withStatement(stmt => {
            stmt.executeQuery(s"SHOW PARTITIONS ON `$dbName`")
                .toIterator
                .filter(r => r.getString("Role") == "Master")
                .map(r => {
                  val (ordinal, host, port) = (r.getInt("Ordinal"), r.getString("Host"), r.getInt("Port"))
                  val partitionQuery = MemSQLRDD.getPerPartitionSql(template, dbName, ordinal)
                  val connInfo = cluster.getMasterInfo.copy(
                    dbHost=host,
                    dbPort=port,
                    dbName=dbName + "_" + ordinal)

                  new MemSQLRDDPartition(ordinal, connInfo, Some(partitionQuery))
                })
                .toArray
          })
        }
      }
    }
  }

  override def compute(sparkPartition: Partition, context: TaskContext): Iterator[T] = new NextIterator[T] {
    context.addTaskCompletionListener(context => closeIfNeeded())

    val partition: MemSQLRDDPartition = sparkPartition.asInstanceOf[MemSQLRDDPartition]

    val (query, queryParams) =
      if (partition.query.isEmpty) {
        (sql, sqlParams)
      } else {
        (partition.query.get, Nil)
      }

    val conn = MemSQLConnectionPool.connect(partition.connectionInfo)
    val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stmt.fillParams(queryParams)

    val rs = stmt.executeQuery

    override def getNext: T = {
      if (rs.next()) {
        mapRow(rs)
      } else {
        finished = true
        null.asInstanceOf[T]
      }
    }

    override def close() {
      try {
        if (stmt != null && !stmt.isClosed) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (conn != null && !conn.isClosed) {
          conn.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }
  }

  override def getPreferredLocations(sparkPartition: Partition): Seq[String] = {
    val partition = sparkPartition.asInstanceOf[MemSQLRDDPartition]
    Seq(partition.connectionInfo.dbHost)
  }

}

object MemSQLRDD {
  def resultSetToObjectArray(rs: ResultSet): Array[Object] = {
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }

  def getPerPartitionSql(template: String, dbName: String, idx: Int): String = {
    // The EXPLAIN query that we run in getPartitions gives us the SQL query
    // that will be run against MemSQL partition number 0; we want to run this
    // query against an arbitrary partition, so we replace the database name
    // in this partition (which is in the form {dbName}_0) with {dbName}_{i}
    // where i is our partition index.
    val dbNameRegex = (dbName + "_0").r
    dbNameRegex.replaceAllIn(template, dbName + "_" + idx)
  }

}
