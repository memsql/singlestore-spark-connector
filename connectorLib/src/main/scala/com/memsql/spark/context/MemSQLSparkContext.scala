package com.memsql.spark.context

import com.memsql.spark.connector.rdd.MemSQLRDD
import com.memsql.spark.connector.dataframe.MemSQLDataFrame

import org.apache.spark._
import org.apache.spark.sql._
import java.sql.{Connection, Statement, ResultSet, DriverManager}
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.util.Random

/**
 * A MemSQL cluster aware version of the SparkContext.
 * 
 * @param conf the SparkConf
 * @param masterAgg the (Host,Port) of the master agg
 * @param childAggs an array of (Host,Port) pairs of the child aggs
 * @param leaves an array of (Host,Port) of leaves
 */
class MemSQLSparkContext(conf: SparkConf,
                         masterAggHost: String,
                         masterAggPort: Int,
                         userName: String,
                         password: String
                       ) extends SparkContext(conf)
{    
    var childAggs = Array[(String,Int)]()
    var leaves    = Array[(String,Int)]()
    private val lock = new ReentrantReadWriteLock()
    UpdateMemSQLTopology

    /*
     * Queries the Master Aggregator for MemSQL topology.
     * DEVNOTE: must be called if MemSQL Topology changes.  
     */
    def UpdateMemSQLTopology : Unit = 
    {
        var newAggs = Array[(String,Int)]()
        var newLeaves = Array[(String,Int)]()
        var conn: Connection = null
        var stmt: Statement = null
        try
        {
            conn = GetMAConnection
            stmt = conn.createStatement
            for (agg <- MemSQLRDD.resultSetToIterator(stmt.executeQuery("SHOW AGGREGATORS")))
            {
                if (agg.getInt("Master_Aggregator") == 0 && agg.getString("State").equals("online"))
                {
                    newAggs = newAggs :+ (agg.getString("Host"), agg.getInt("Port"))
                    if (masterAggHost == "127.0.0.1" && agg.getString("Host") != "127.0.0.1")
                    {
                        throw new SparkException("Please create MemSQLSparkContext with masterAggHost set to a cluster-visible IP (not 127.0.0.1)")
                    }
                }
            }
            for (leaf <- MemSQLRDD.resultSetToIterator(stmt.executeQuery("SHOW LEAVES")))
            {
                if (leaf.getString("State").equals("online"))
                {
                    newLeaves = newLeaves :+ (leaf.getString("Host"), leaf.getInt("Port"))
                    if (masterAggHost == "127.0.0.1" && leaf.getString("Host") != "127.0.0.1")
                    {
                        throw new SparkException("Please create MemSQLSparkContext with masterAggHost set to a cluster-visible IP (not 127.0.0.1)")
                    }
                }
            }            
            lock.writeLock.lock
            childAggs = newAggs
            leaves = newLeaves
            lock.writeLock.unlock
        }
        finally
        {
            if (stmt != null && !stmt.isClosed()) 
            {
                stmt.close()
            }
            if (conn != null && !conn.isClosed()) 
            {
                conn.close()
            }
        }
    }

    def GetMAConnection: Connection =
    {
        val dbAddress = "jdbc:mysql://" + masterAggHost + ":" + masterAggPort
        DriverManager.getConnection(dbAddress, userName, password)
    }

    /*
     * Returns a list of the (host,port) pairs of the nodes which can accept writes.
     * The master agg will only be included if no other nodes are available.
     *
     * For now, returns child aggs, but ideally will return leaves.  
     */
    def GetMemSQLNodesAvailableForIngest: Array[(String,Int)] = 
    {
        lock.readLock.lock
        val result = if (childAggs.size == 0)
        {
            Array((masterAggHost,masterAggPort)) 
        }
        else // When loading into the leaves happens, this can just be leaves + childAggs 
        {
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
     */  
    def GetMemSQLNodesAvailableForRead: Array[(String,Int)] = 
    {
        GetMemSQLNodesAvailableForIngest // currently nodes available for read are exactly those available for writes.
    }
    
    def GetMemSQLLeaves: Array[(String,Int)] = leaves
    def GetMemSQLChildAggregators: Array[(String,Int)] = childAggs
    def GetMemSQLMasterAggregator: (String,Int) = (masterAggHost,masterAggPort)
    def GetUserName: String = userName
    def GetPassword: String = password

    /*
     * Creates a MemSQLRDD from a select query.
     * If the query is fully pushed down, the RDD will have one Spark partition per MemSQL partition,
     * Else it will have a single spark partition.
     */
    def CreateRDDFromMemSQLQuery(dbName: String, query: String): MemSQLRDD[Row] =
    {
        val aggs = GetMemSQLNodesAvailableForRead
        val agg = aggs(Random.nextInt(aggs.size))
        MemSQLDataFrame.MakeMemSQLRowRDD(this, agg._1, agg._2, userName, password, dbName, query)
    }
}
