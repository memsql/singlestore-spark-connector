package com.memsql.spark.context

import scala.util.Random
import java.sql._
import java.util.concurrent.locks._
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

trait MemSQLMetaData {
  def getMemSQLMasterAggregator: MemSQLNode
  def getMemSQLChildAggregators: List[MemSQLNode]
  def getMemSQLLeaves: List[MemSQLNode]
  def getMemSQLUserName: String
  def getMemSQLPassword: String
  def getMemSQLNodesAvailableForIngest: List[MemSQLNode]
  def getMemSQLNodesAvailableForRead: List[MemSQLNode]
}

object MemSQLContext {
  def apply(sparkContext: SparkContext) = {
    val conf = sparkContext.getConf
    val dbHost = conf.get("memsql.host", "127.0.0.1")
    val dbPort = conf.getInt("memsql.port", 3306)
    val dbUser = conf.get("memsql.user", "root")
    val dbPassword = conf.get("memsql.password", "")
    new MemSQLContext(sparkContext, dbHost, dbPort, dbUser, dbPassword)
  }
}

/**
 * A MemSQL cluster aware version of the SQLContext.
 *
 * @param sparkContext the SparkContext
 * @param masterAggHost the host of the master agg
 * @param masterAggPort the port of the master agg
 * @param userName the user name for the master agg
 * @param password the password for the master agg
 */
class MemSQLContext(sparkContext: SparkContext,
                    masterAggHost: String,
                    masterAggPort: Int,
                    userName: String,
                    password: String) extends SQLContext(sparkContext) with MemSQLMetaData {

  var masterAgg = MemSQLNode(masterAggHost, masterAggPort, MemSQLRole.Master)
  var childAggs = List[MemSQLNode]()
  var leaves    = List[MemSQLNode]()
  private val lock = new ReentrantReadWriteLock()
  private var topologyUpdatedOnce = false

  override def getMemSQLMasterAggregator: MemSQLNode = masterAgg

  override def getMemSQLChildAggregators: List[MemSQLNode] = {
    ensureTopologyUpdatedAtLeastOnce
    lock.readLock.lock
    val children = childAggs
    lock.readLock.unlock
    children
  }

  override def getMemSQLLeaves: List[MemSQLNode] = {
    ensureTopologyUpdatedAtLeastOnce
    lock.readLock.lock
    val leafNodes = leaves
    lock.readLock.unlock
    leafNodes
  }

  override def getMemSQLUserName: String = userName
  override def getMemSQLPassword: String = password

  /*
   * Returns a list of the (host,port) pairs of the nodes which can accept writes.
   * The master agg will only be included if no other nodes are available.
   *
   * For now, returns child aggs, but ideally will return leaves.
   */
  override def getMemSQLNodesAvailableForIngest: List[MemSQLNode] = {
    ensureTopologyUpdatedAtLeastOnce
    lock.readLock.lock
    val result = if (childAggs.size == 0) {
      List(masterAgg)
    } else { // When loading into the leaves happens, this can just be leaves + childAggs
      childAggs
    }
    lock.readLock.unlock
    result
  }

  /*
   * Returns a list of the (host,port) pairs of the nodes which can accept reads.
   * The master agg will only be included if no other nodes are available.
   *
   * For now, returns child aggs, but ideally will return leaves.
   * Currently nodes available for read are exactly those available for writes.
   */
  override def getMemSQLNodesAvailableForRead: List[MemSQLNode] = getMemSQLNodesAvailableForIngest

  def getMAConnection: Connection = {
    val dbAddress = "jdbc:mysql://" + masterAggHost + ":" + masterAggPort
    DriverManager.getConnection(dbAddress, userName, password)
  }

  /*
   * Queries the Master Aggregator for MemSQL topology.
   * DEVNOTE: must be called if MemSQL Topology changes.
   */
  def updateMemSQLTopology: Unit = {
    var newAggs = List[MemSQLNode]()
    var newLeaves = List[MemSQLNode]()
    var conn: Connection = null
    var stmt: Statement = null
    try {
      conn = getMAConnection
      stmt = conn.createStatement
      for (agg <- MemSQLRDD.resultSetToIterator(stmt.executeQuery("SHOW AGGREGATORS"))) {
        if (agg.getInt("Master_Aggregator") == 0 && agg.getString("State").equals("online")) {
          newAggs = newAggs :+ MemSQLNode(agg.getString("Host"), agg.getInt("Port"), MemSQLRole.Child)
          if (masterAggHost == "127.0.0.1" && agg.getString("Host") != "127.0.0.1") {
            throw new SparkException("Please create MemSQLContext with masterAggHost set to a cluster-visible IP (not 127.0.0.1)")
          }
        }
      }
      for (leaf <- MemSQLRDD.resultSetToIterator(stmt.executeQuery("SHOW LEAVES"))) {
        if (leaf.getString("State").equals("online")) {
          newLeaves = newLeaves :+ MemSQLNode(leaf.getString("Host"), leaf.getInt("Port"), MemSQLRole.Leaf)
          if (masterAggHost == "127.0.0.1" && leaf.getString("Host") != "127.0.0.1") {
            throw new SparkException("Please create MemSQLContext with masterAggHost set to a cluster-visible IP (not 127.0.0.1)")
          }
        }
      }
      lock.writeLock.lock
      childAggs = newAggs
      leaves = newLeaves
      lock.writeLock.unlock
    } finally {
      if (stmt != null && !stmt.isClosed()) {
        stmt.close()
      }
      if (conn != null && !conn.isClosed()) {
        conn.close()
      }
    }
  }

  def createDataFrameFromMemSQLTable(dbName: String, tableName: String) : DataFrame = {
    createDataFrameFromMemSQLQuery(dbName, "SELECT * FROM " + tableName)
  }

  def createDataFrameFromMemSQLQuery(dbName: String, query: String) : DataFrame = {
    val aggs = getMemSQLNodesAvailableForRead
    val agg = aggs(Random.nextInt(aggs.size))
    MemSQLDataFrame.MakeMemSQLDF(this, agg.host, agg.port, userName, password, dbName, query)
  }

  def getTableSchema(dbName: String, tableName: String) : StructType = createDataFrameFromMemSQLTable(dbName, tableName).schema

  /*
   * Creates a MemSQLRDD from a select query.
   * If the query is fully pushed down, the RDD will have one Spark partition per MemSQL partition,
   * Else it will have a single spark partition.
   */
  def createRDDFromMemSQLQuery(dbName: String, query: String): MemSQLRDD[Row] = {
    val aggs = getMemSQLNodesAvailableForRead
    val agg = aggs(Random.nextInt(aggs.size))
    MemSQLDataFrame.MakeMemSQLRowRDD(sparkContext, agg.host, agg.port, userName, password, dbName, query)
  }

  private def ensureTopologyUpdatedAtLeastOnce: Unit = {
    if (!topologyUpdatedOnce) {
      updateMemSQLTopology
      topologyUpdatedOnce = true
    }
  }
}
