package com.memsql.spark.context

import com.memsql.spark.connector.rdd.MemSQLRDD
import com.memsql.spark.connector.dataframe.MemSQLDataFrame

import org.apache.spark._
import org.apache.spark.sql._

import scala.util.Random

/*
 * A convenience class extending Spark's SQLContext.
 * Makes it easy to create dataframes from MemSQL.
 */
class MemSQLSQLContext(sparkContext: MemSQLSparkContext) extends SQLContext(sparkContext)
{
    def CreateDataFrameFromMemSQLTable(dbName: String, tableName: String) : DataFrame =
    {
        CreateDataFrameFromMemSQLQuery(dbName, "SELECT * FROM " + tableName)
    }
    def CreateDataFrameFromMemSQLQuery(dbName: String, query: String) : DataFrame =
    {
        val aggs = sparkContext.GetMemSQLNodesAvailableForRead
        val agg = aggs(Random.nextInt(aggs.size))
        MemSQLDataFrame.MakeMemSQLDF(
          this,
          agg._1,
          agg._2,
          sparkContext.GetUserName,
          sparkContext.GetPassword,
          dbName,
          query)
    }
}
