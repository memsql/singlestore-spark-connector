// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark.connector.sql

import org.scalatest.FlatSpec

/**
  * Checks that dataframes from MemSQL are still accurate when
  * disablePartitionPushdown option is set to true,
  * and that the RDD only has 1 partition
  */
class DisablePartitionPushdownOptionSpec extends FlatSpec with SharedMemSQLContext{
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
      .options(Map( "path" -> (dbName + ".pushdown"), "disablePartitionPushdown" -> "true"))
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
