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
    val connInfo = MemSQLConnectionInfo("127.0.0.1", 3306, "root", "", "information_schema")

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
        stmt.execute("CREATE DATABASE IF NOT EXISTS db")
        stmt.execute("DROP TABLE IF EXISTS db.t")
        stmt.execute("CREATE TABLE db.t (data VARCHAR(200), SHARD KEY (data))")
      })
    })

    /*
    Create and save the dataframe:
    +------------------+
    | data             |
    +------------------+
    |  test_data_00    |
    |  test_data_01    |
    |  ...             |
    |  test_data_09    |
    +------------------+
    into MemSQL as db.t
    */
    val rdd = ss.sparkContext.parallelize(Range(0, 10).map(i => Row("test_data_%02d".format(i))))
    val schema = StructType(Seq(StructField("data", StringType, false)))
    val df = ss.createDataFrame(rdd, schema)
    df.saveToMemSQL("db", "t")

    /* Read the table from MemSQL back into Spark, and save it to HDFS as a parquet file */
    val df_2 = spark.read
      .format("com.memsql.spark.connector")
      .options(Map("path" -> ("db.t")))
      .load()

    df_2.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("hdfs://1.3.3.7/test_data.parquet") /* HDFS path to save file */

    /* Load dataframe from HDFS, and save to MemSQL again */
    val df_3 = spark.read.parquet("hdfs://1.3.3.7/test_data.parquet")
    df_3.saveToMemSQL("db", "t")
  }
}
