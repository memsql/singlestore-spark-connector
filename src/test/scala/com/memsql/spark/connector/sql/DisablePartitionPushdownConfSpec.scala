// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark.connector.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

/**
  * Checks that dataframes from MemSQL are still accurate when
  * disablePartitionPushdown Spark configuration is set, and that the RDD only has 1 partition
  */
class DisablePartitionPushdownConfSpec extends FlatSpec with SharedMemSQLContext{
  override def sparkUp(local: Boolean=false): Unit = {
    recreateDatabase

    var conf = new SparkConf()
      .setAppName("MemSQL Connector Test")
      .set("spark.memsql.host", masterConnectionInfo.dbHost)
      .set("spark.memsql.port", masterConnectionInfo.dbPort.toString)
      .set("spark.memsql.user", masterConnectionInfo.user)
      .set("spark.memsql.password", masterConnectionInfo.password)
      .set("spark.memsql.defaultDatabase", masterConnectionInfo.dbName)
      .set("spark.memsql.disablePartitionPushdown", "true")

    if (local) {
      conf = conf.setMaster("local")
    }

    ss = SparkSession.builder().config(conf).getOrCreate()
    sc = ss.sparkContext
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    withStatement(masterConnectionInfo)(stmt => {
      stmt.execute("CREATE TABLE pushdown(id INT PRIMARY KEY, data VARCHAR(200), key(data))")

      val insertValues = Range(0, 20)
        .map(i => s"""($i, 'test_data_${"%02d".format(i)}')""")
        .mkString(",")

      stmt.execute("INSERT INTO pushdown VALUES" + insertValues)
    })
  }

  "Disabling partition pushdown in options" should "still return the correct result" in {
    val pushdownDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".pushdown")))
      .load()

    assert(pushdownDataframe.rdd.getNumPartitions == 1)

    val data = pushdownDataframe.collect().map(x => x.getString(1)).sortWith(_ < _)

    assert (data.length == 20)
    assert(data(0).equals("test_data_00"))
    assert(data(1).equals("test_data_01"))
    assert(data(2).equals("test_data_02"))
    assert(data(3).equals("test_data_03"))
  }
}

