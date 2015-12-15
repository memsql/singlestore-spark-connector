package com.memsql.spark.examples

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import com.memsql.spark.connector._
import com.memsql.spark.connector.util.JDBCImplicits._

object WriteToMemSQLApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Write to MemSQL Example")
    val sc = new SparkContext(conf)
    val msc = new MemSQLContext(sc)

    val dbName = "memsqlrdd_db"
    val tableName = "output"

    msc.getMemSQLCluster.withMasterConn(conn => {
      conn.withStatement(stmt => {
        stmt.execute(s"CREATE DATABASE IF NOT EXISTS $dbName")
        stmt.execute(s"DROP TABLE IF EXISTS $dbName.$tableName")
        stmt.execute(s"""
          CREATE TABLE $dbName.$tableName
          (data VARCHAR(200), SHARD KEY (data))
        """)
      })
    })

    val rows = Range(0, 1000).map(i => Row("test_data_%04d".format(i))) // scalastyle:ignore
    val rdd = sc.parallelize(rows)
    val schema = msc.table(s"$dbName.$tableName").schema
    msc.createDataFrame(rdd, schema).saveToMemSQL(dbName, tableName)
  }
}
