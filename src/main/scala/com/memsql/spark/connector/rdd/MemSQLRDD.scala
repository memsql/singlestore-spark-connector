package com.memsql.spark.connector.rdd

import java.sql.ResultSet

import com.memsql.spark.connector.{MemSQLCluster, MemSQLConnectionPool}
import java.sql.Connection

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import com.memsql.spark.connector.util.{MemSQLConnectionInfo, NextIterator}
import com.memsql.spark.connector.util.JDBCImplicits._
import spray.json._
import spray.json.DefaultJsonProtocol._

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
  *   Any.
  */
case class MemSQLRDD[T: ClassTag](@transient sc: SparkContext,
                                  cluster: MemSQLCluster,
                                  sql: String,
                                  sqlParams: Seq[Any] = Nil,
                                  databaseName: Option[String] = None,
                                  mapRow: (ResultSet) => T = MemSQLRDD.resultSetToArray _,
                                  disablePartitionPushdown: Boolean = false,
                                  enableStreaming: Boolean = false
                                 ) extends RDD[T](sc, Nil){

  override def getPartitions: Array[Partition] = {
    if(disablePartitionPushdown) {
      getSinglePartition
    } else {
      // databaseName is required for partition pushdown
      databaseName match {
        case None => getSinglePartition
        case Some(dbName) => {
          cluster.withMasterConn[Array[Partition]](conn => {
            val partitionQueryFn = getPartitionQueryFn(conn, dbName)

            conn.withStatement(stmt => {
              partitionQueryFn match {
                case Some(pq) => {
                  stmt.executeQuery(s"SHOW PARTITIONS ON `${dbName}`")
                    .toIterator
                    .filter(r => r.getString("Role") == "Master")
                    .map(r => {
                      val (ordinal, host, port) = (r.getInt("Ordinal"), r.getString("Host"), r.getInt("Port"))

                      val partitionQuery = pq(ordinal)

                      val connInfo = cluster.getMasterInfo.copy(
                        dbHost = host,
                        dbPort = port,
                        dbName = MemSQLRDD.getDatabaseShardName(dbName, ordinal))

                      new MemSQLRDDPartition(ordinal, connInfo, Some(partitionQuery))
                    })
                    .toArray.sortBy(_.index).asInstanceOf[Array[Partition]]
                }
                case None => {
                  getSinglePartition
                }
              }
            })
          })
        }
      }
    }
  }

  private def getSinglePartition: Array[Partition] = {
    Array[Partition](new MemSQLRDDPartition(0, cluster.getRandomAggregatorInfo))
  }

  // Generate EXPLAIN output by one of two methods, depending on the version of MemSQL.
  // Inspect the explain output to determine if we can safely pushdown the query to the leaves.
  private def getPartitionQueryFn(conn: Connection, dbName: String): Option[(Int) => String] = {
    val explainExtendedSQL = "EXPLAIN EXTENDED " + sql
    val explainJsonSQL = "EXPLAIN JSON " + sql

    conn.withPreparedStatement(explainExtendedSQL, stmt => {
      stmt.fillParams(sqlParams)
      val explainExtendedResult = stmt.executeQuery

      if (explainExtendedResult.hasColumn("Query")) {
        getPartitionQueryFnFromExplainExtended(explainExtendedResult, dbName)

      } else {
        conn.withPreparedStatement(explainJsonSQL, stmt => {
          stmt.fillParams(sqlParams)
          val explainJsonResult = stmt.executeQuery

          if (explainJsonResult.hasColumn("EXPLAIN")) {
            getPartitionQueryFnFromExplainJson(explainJsonResult, dbName)

          } else {
            None
          }
        })
      }
    })

  }

  private def getPartitionQueryFnFromExplainExtended(explainResult: ResultSet,
                                                     dbName: String): Option[(Int) => String] = {
    val explainRows = explainResult
      .toIterator
      .map(r => {
        val selectType = r.getString("select_type")
        val extra = r.getString("Extra")
        val query = r.getString("Query")
        ExplainRow(selectType, extra, query)
      })
      .toList

    // Here, we only pushdown queries which have a single SIMPLE
    // query, and are a Simple Iterator on the agg.
    val isSimpleIterator = explainRows.headOption.exists(_.extra == "memsql: Simple Iterator -> Network")
    val hasMultipleRows = explainRows.length > 1
    val noDResult = !explainRows.exists(_.selectType == "DRESULT")

    if (isSimpleIterator && hasMultipleRows && noDResult) {
      val template = explainRows(1).query

      Some(partitionIdx =>
        MemSQLRDD.getPerPartitionSql(template, dbName, partitionIdx)
      )
    } else {
      None
    }
  }

  private def getPartitionQueryFnFromExplainJson(explainResult: ResultSet,
                                                 dbName: String): Option[(Int) => String] = {
    val jsonStr = explainResult.toIterator
      .map(_.getString("EXPLAIN"))
      .mkString("\n")

    // Here, we only pushdown queries with whitelisted executors.
    val executorWhitelist = Set(
      "Project",
      "Gather",
      "Filter",
      "TableScan",
      "ColumnStoreScan",
      "OrderedColumnStoreScan",
      "IndexRangeScan",
      "IndexSeek",
      "NestedLoopJoin",
      "ChoosePlan",
      "HashGroupBy",
      "StreamingGroupBy")

    def extractExplain(json: JsValue): JsValue = {
      // Feature-detect the explain format version.
      val fields = json.asJsObject.fields
      if (fields contains "version") {
        // In explain format version 2 (introduced in MemSQL 6.7), the output
        // we want is nested under the "explain" key as the only element of an
        // array.
        fields("explain").convertTo[JsArray].elements(0)
      } else {
        // If no "version" key exists, this is format version 1 (MemSQL 6.5.x
        // and below), which is exactly the output we want.
        json
      }
    }
    val jsonAst = extractExplain(jsonStr.parseJson)

    // Traverses the "inputs" fields of EXPLAIN JSON output to find values of the given key.
    def getFields(key: String, jsonAst: JsValue): Seq[String] = {
      // Assumes that the JsValue (i.e. AST node) at this level is a JsObject (i.e. dict), not a JsArray.
      // Throws a DeserializationException if this assumption is false.
      val jsonMap = jsonAst.asJsObject.fields

      // Read the key at the top-level JSON object.
      val head = if (jsonMap contains key) {
        Seq(jsonMap(key).convertTo[String])
      } else {
        Nil
      }

      // Recurse down to each JsObject in "inputs".
      val tail = if (jsonMap contains "inputs") {
        val inputs = jsonMap("inputs").convertTo[JsArray].elements
        inputs.flatMap(getFields(key, _))
      } else {
        Nil
      }

      head ++ tail
    }

    // Check there is only one gather and it either first or only preceeded by project
    def checkGather(executors: Seq[String]): Boolean = {
      val numGather = executors.count(executor => executor.toLowerCase == "gather")
      if (numGather != 1) {
        false
      } else {
        /* Check that gather is only preceeded by project */
        val dropProject = executors.dropWhile(executor => executor.toLowerCase == "project")
        if (dropProject(0).toLowerCase == "gather"){
          true
        } else {
          false
        }
      }
    }

    // Examine the EXPLAIN output. If all executors are amenable to pushdown, and if there is a single "query" field
    // in the EXPLAIN output, return that query.
    val generatedQuery: Option[String] = {
      val noBadExecutors = {
        val executors = Try(getFields("executor", jsonAst))

        executors.map(checkGather(_)).getOrElse(false) && executors.map(_.forall(executorWhitelist.contains)).getOrElse(false)
      }

      val generatedQueries = Try(getFields("query", jsonAst)).getOrElse(Seq())
      if (noBadExecutors && generatedQueries.length == 1) {
        generatedQueries.headOption
      } else {
        None
      }
    }

    // The EXPLAIN query gives us a USING ... SELECT ...
    // statement; we pull out the SELECT statement.
    generatedQuery.flatMap(q => {
      val selectIndex = q.indexOfSlice("SELECT")
      val generatedSelectClause = q.slice(selectIndex, q.length)

      Some(partitionIdx =>
        MemSQLRDD.getPerPartitionSql(generatedSelectClause, dbName, partitionIdx)
      )
    })
  }

  override def compute(sparkPartition: Partition, context: TaskContext): Iterator[T] = new NextIterator[T] {
    context.addTaskCompletionListener(context => closeIfNeeded())

    val partition: MemSQLRDDPartition = sparkPartition.asInstanceOf[MemSQLRDDPartition]

    val (query, queryParams) = partition.query match {
      case Some(partitionQuery) => (partitionQuery, Nil)
      case None => (sql, sqlParams)
    }

    val conn = MemSQLConnectionPool.connect(partition.connectionInfo)
    val stmt = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    if (enableStreaming) {
      // setFetchSize(Integer.MIN_VALUE) enables row-by-row streaming for the MySQL JDBC connector
      // https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-implementation-notes.html
      // Currently, it does not allow custom fetch size
      stmt.setFetchSize(Integer.MIN_VALUE)
    }
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
  def resultSetToArray(rs: ResultSet): Array[Any] = rs.toArray

  def getDatabaseShardName(dbName: String, idx: Int): String = {
    dbName + "_" + idx
  }

  def getPerPartitionSql(template: String, dbName: String, idx: Int): String = {
    // The EXPLAIN query that we run in getPartitions gives us the SQL query
    // that will be run against MemSQL partition number 0; we want to run this
    // query against an arbitrary partition, so we replace the database name
    // in this partition (which is in the form {dbName}_0) with {dbName}_{i}
    // where i is our partition index.
    val dbNameRegex = getDatabaseShardName(dbName, 0).r
    dbNameRegex.replaceAllIn(template, getDatabaseShardName(dbName, idx))
  }
}
