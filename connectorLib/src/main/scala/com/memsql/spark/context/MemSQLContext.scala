package com.memsql.spark.context

import com.memsql.spark.connector.util.MemSQLConnectionInfo
import com.memsql.spark.pushdown.MemSQLPushdownStrategy

import scala.util.Random
import java.sql._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.memsql.spark.connector.rdd._
import com.memsql.spark.connector.dataframe._

object MemSQLRole extends Enumeration {
  type MemSQLRole = Value
  val Master, Child, Leaf = Value
}
import MemSQLRole._

case class MemSQLNode(host: String, port: Int, role: MemSQLRole)

object MemSQLContext {
  val DEFAULT_HOST = "127.0.0.1"
  val DEFAULT_PORT = 3306
  val DEFAULT_USER = "root"
  val DEFAULT_PASSWORD = ""
  val DEFAULT_JDBC_LOGIN_TIMEOUT = 10 //seconds

  def apply(sparkContext: SparkContext, enablePushdown: Boolean = true): MemSQLContext = {
    val conf = sparkContext.getConf
    val dbHost = conf.get("memsql.host", DEFAULT_HOST)
    val dbPort = conf.getInt("memsql.port", DEFAULT_PORT)
    val dbUser = conf.get("memsql.user", DEFAULT_USER)
    val dbPassword = conf.get("memsql.password", DEFAULT_PASSWORD)
    new MemSQLContext(sparkContext, dbHost, dbPort, dbUser, dbPassword, enablePushdown)
  }

  def getMemSQLConnection(host: String, port: Int, userName: String, password: String, dbName: String = null): Connection = {
    val dbAddress = "jdbc:mysql://" + host + ":" + port + (if (dbName != null)
      {
        "/" + dbName
      } else {
        ""
      })

    // set a sane non-zero login timeout before connecting
    DriverManager.setLoginTimeout(DEFAULT_JDBC_LOGIN_TIMEOUT)
    DriverManager.getConnection(dbAddress, userName, password)
  }

  def getMemSQLChildAggregators(masterHost: String, masterPort: Int, userName: String, password: String): List[MemSQLNode] = {
    var newAggs = List[MemSQLNode]()
    var conn: Connection = null
    var stmt: Statement = null
    try {
      conn = getMemSQLConnection(masterHost, masterPort, userName, password)
      stmt = conn.createStatement
      for (agg <- MemSQLRDD.resultSetToIterator(stmt.executeQuery("SHOW AGGREGATORS"))) {
        if (agg.getInt("Master_Aggregator") == 0 && agg.getString("State").equals("online")) {
          newAggs = newAggs :+ MemSQLNode(agg.getString("Host"), agg.getInt("Port"), MemSQLRole.Child)
          if (masterHost == "127.0.0.1" && agg.getString("Host") != "127.0.0.1") {
            throw new SparkException("Please create MemSQLContext with masterHost set to a cluster-visible IP (not 127.0.0.1)")
          }
        }
      }
      newAggs
    } finally {
      if (stmt != null && !stmt.isClosed()) {
        stmt.close()
      }
      if (conn != null && !conn.isClosed()) {
        conn.close()
      }
    }
  }

  def getMemSQLLeaves(masterHost: String, masterPort: Int, userName: String, password: String): List[MemSQLNode] = {
    var newLeaves = List[MemSQLNode]()
    var conn: Connection = null
    var stmt: Statement = null
    try {
      conn = getMemSQLConnection(masterHost, masterPort, userName, password)
      stmt = conn.createStatement
      for (leaf <- MemSQLRDD.resultSetToIterator(stmt.executeQuery("SHOW LEAVES"))) {
        if (leaf.getString("State").equals("online")) {
          newLeaves = newLeaves :+ MemSQLNode(leaf.getString("Host"), leaf.getInt("Port"), MemSQLRole.Leaf)
          if (masterHost == "127.0.0.1" && leaf.getString("Host") != "127.0.0.1") {
            throw new SparkException("Please create MemSQLContext with masterHost set to a cluster-visible IP (not 127.0.0.1)")
          }
        }
      }
      newLeaves
    } finally {
      if (stmt != null && !stmt.isClosed()) {
        stmt.close()
      }
      if (conn != null && !conn.isClosed()) {
        conn.close()
      }
    }
  }

  /*
   * Returns a list of the child aggregators which can accept writes.
   * The master agg will only be included if no other nodes are available.
   */
  def getMemSQLNodesAvailableForIngest(masterHost: String,
                                       masterPort: Int,
                                       userName: String,
                                       password: String,
                                       alwaysIncludeMaster: Boolean=false): List[MemSQLNode] = {

    val childAggs = getMemSQLChildAggregators(masterHost, masterPort, userName, password)
    if (childAggs.size == 0) {
      List(MemSQLNode(masterHost, masterPort, MemSQLRole.Master))
    } else if (alwaysIncludeMaster) {
      childAggs ++ List(MemSQLNode(masterHost, masterPort, MemSQLRole.Master))
    } else {
      childAggs
    }
  }
}

/**
 * A MemSQL cluster aware version of the SQLContext.
 *
 * @param sparkContext The SparkContext used to create this SQLContext.
 * @param masterAggHost The host of the master aggregator.
 * @param masterAggPort The port of the master aggregator.
 * @param userName The user name for the master aggregator.
 * @param password The password for the master aggregator.
 * @param enablePushdown When set, certain dataframe operations will be pushed down into underlying MemSQL Queries.
 */
class MemSQLContext(sparkContext: SparkContext,
                    masterAggHost: String,
                    masterAggPort: Int,
                    userName: String,
                    password: String,
                    enablePushdown: Boolean = true) extends SQLContext(sparkContext) {

  var masterAgg = MemSQLNode(masterAggHost, masterAggPort, MemSQLRole.Master)

  val pushdownEnabled = enablePushdown
  if (pushdownEnabled) {
    MemSQLPushdownStrategy.patchSQLContext(this)
  }

  def getMemSQLMasterAggregator: MemSQLNode = masterAgg

  def getMemSQLChildAggregators: List[MemSQLNode] = {
    MemSQLContext.getMemSQLChildAggregators(masterAgg.host, masterAgg.port, userName, password)
  }

  def getMemSQLLeaves: List[MemSQLNode] = {
    MemSQLContext.getMemSQLLeaves(masterAgg.host, masterAgg.port, userName, password)
  }

  def getMemSQLUserName: String = userName
  def getMemSQLPassword: String = password

  def getMemSQLNodesAvailableForIngest(alwaysIncludeMaster: Boolean = false): List[MemSQLNode] = {
    MemSQLContext.getMemSQLNodesAvailableForIngest(masterAgg.host, masterAgg.port, userName, password, alwaysIncludeMaster)
  }

  /*
   * Returns a list of nodes which can accept reads.
   * Currently nodes available for read are exactly those available for writes.
   */
  def getMemSQLNodesAvailableForRead: List[MemSQLNode] = getMemSQLNodesAvailableForIngest()

  def createDataFrameFromMemSQLTable(dbName: String, tableName: String) : DataFrame = {
    createDataFrameFromMemSQLQuery(dbName, "SELECT * FROM " + tableName)
  }

  def createDataFrameFromMemSQLQuery(dbName: String, query: String, queryParams: Seq[Object]=Nil) : DataFrame = {
    val aggs = getMemSQLNodesAvailableForRead
    val agg = aggs(Random.nextInt(aggs.size))
    val cxnInfo = MemSQLConnectionInfo(agg.host, agg.port, userName, password, dbName)
    MemSQLDataFrame.UnsafeMakeMemSQLDF(this, cxnInfo, query, queryParams)
  }

  def getTableSchema(dbName: String, tableName: String) : StructType =
    createDataFrameFromMemSQLTable(dbName, tableName).schema

  /**
   * Returns a JDBC Connection to a MemSQLNode.
   *
   * @param node The node to connect to.  If omitted or null, will return a connection to the Master Aggregator
   * @param dbName The optional name of the database to connect to.
   */
  def getMemSQLConnection(node: MemSQLNode = null, dbName: String = null): Connection = {
    val theNode = if (node == null) {
      masterAgg
    } else {
      node
    }
    MemSQLContext.getMemSQLConnection(theNode.host, theNode.port, userName, password, dbName)
  }

}
