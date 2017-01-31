package com.memsql.spark.examples

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import com.memsql.spark.connector._
import com.memsql.spark.connector.util._
import com.memsql.spark.connector.util.JDBCImplicits._

object WriteToMemSQLApp {
  def main(args: Array[String]) {
    val connInfo = MemSQLConnectionInfo("127.0.0.1", 3306, "root", "", "information_schema") // scalastyle:ignore

    val dbName = "memsqlrdd_db"
    val tableName = "output"

    var conf = new SparkConf()
      .setAppName("Write to MemSQL Example")
      .set("spark.memsql.host", connInfo.dbHost)
      .set("spark.memsql.port", connInfo.dbPort.toString)
      .set("spark.memsql.user", connInfo.user)
      .set("spark.memsql.password", connInfo.password)
      .set("spark.memsql.defaultDatabase", connInfo.dbName)

    val ss = SparkSession.builder().config(conf).getOrCreate()
    import ss.implicits._

    MemSQLConnectionPool.withConnection(connInfo)(conn => {
      conn.withStatement(stmt => {
        stmt.execute(s"CREATE DATABASE IF NOT EXISTS $dbName")
        stmt.execute(s"DROP TABLE IF EXISTS $dbName.$tableName")
        stmt.execute(s"""
          CREATE TABLE $dbName.$tableName
          (data VARCHAR(200), SHARD KEY (data))
        """)
      })
    })

    val rdd = ss.sparkContext.parallelize(Range(0, 1000).map(i => Row("test_data_%04d".format(i)))) // scalastyle:ignore
    val schema = StructType(Seq(StructField("data", StringType, false)))
    val df = ss.createDataFrame(rdd, schema)
    df.saveToMemSQL(dbName, tableName)
  }
}
