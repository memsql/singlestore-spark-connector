package com.memsql.spark.connector.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

/**
  * When there is no spark.memsql.defaultDatabase set in the Spark configuration options
  * AND no "database" option set when reading a Dataframe from MemSQL with the user-query option,
  * the connector is unable to load the data in parallel from the leaves.
  * There will thus only be one partition.
  */
class UserQueryOnlyOnePartitionSpec  extends FlatSpec with SharedMemSQLContext{

  override def sparkUp(local: Boolean = false): Unit = {
    recreateDatabase

    var conf = new SparkConf()
      .setAppName("MemSQL Connector Test")
      .set("spark.memsql.host", masterConnectionInfo.dbHost)
      .set("spark.memsql.port", masterConnectionInfo.dbPort.toString)
      .set("spark.memsql.user", masterConnectionInfo.user)
      .set("spark.memsql.password", masterConnectionInfo.password)

    if (local) {
      conf = conf.setMaster("local")
    }

    ss = SparkSession.builder().config(conf).getOrCreate()
    sc = ss.sparkContext
  }

  "A custom-query MemSQL Dataframe" should "only have one partition when there is no database specified" in {

    TestUtils.setupBasic(this)

    val table = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map("query" -> ("SELECT * FROM " + dbName + ".t")))
      .load()
    assert(table.rdd.getNumPartitions == 1)
  }
}
