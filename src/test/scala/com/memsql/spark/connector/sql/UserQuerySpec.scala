// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark.connector.sql

import org.scalatest.FlatSpec

/**
  * Read MemSQL tables as dataframes using the Spark DataSource API and
  * a SQL query to specify the desired data
  */
class UserQuerySpec extends FlatSpec with SharedMemSQLContext{
  override def beforeAll(): Unit = {
    super.beforeAll()

    TestUtils.setupBasic(this)
  }

  "UserQuerySpec" should "create dataframe from user-specified query" in {
    // Verify that we can read from each table
    for (name <- Seq("t", "s", "r")) {
      val table = ss
        .read
        .format("com.memsql.spark.connector")
        .options(Map("query" -> ("SELECT * FROM " + dbName + "." + name)))
        .load()

      assert(table.count == 1000)
      assert(table.schema.exists(f => f.name == "data"))

      val table2 = ss
        .read
        .format("com.memsql.spark.connector")
        .options(Map("query" -> ("SELECT * FROM " + dbName + "." + name + " WHERE id < 3")))
        .load()

      assert(table2.count == 3)
      assert(table2.schema.exists(f => f.name == "data"))

      val table3 = ss
        .read
        .format("com.memsql.spark.connector")
        .options(Map("query" -> ("SELECT * FROM " + dbName + "." + name + " LIMIT 10")))
        .load()

      assert(table3.count == 10)
      assert(table3.schema.exists(f => f.name == "data"))

      val table4 = ss
        .read
        .format("com.memsql.spark.connector")
        .options(Map("query" -> ("SELECT * FROM " + dbName + "." + name + " ORDER BY ID LIMIT 10")))
        .load()

      assert(table4.count == 10)
      assert(table4.schema.exists(f => f.name == "data"))

      val table4_collect = table4.collect()
      for (i <- 0 to 9) {
        assert(table4_collect(i)(0) == i)
      }
    }
  }

  // To enable partition pushdown for custom queries, a database must be specified.
  // First we check if the user has added one to the options
  "A custom-query MemSQL Dataframe" should "have more than one partition if user has specified database name" in {
    val table = ss
        .read
        .format("com.memsql.spark.connector")
        .options(Map("query" -> ("SELECT * FROM t"), "database" -> dbName))
        .load()
    assert(table.rdd.getNumPartitions > 1)
  }

  it should "have more than one partition if user has specified defaultDB in Spark configuration" in {
    val table = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map("query" -> ("SELECT * FROM " + dbName + ".t")))
      .load()
    assert(table.rdd.getNumPartitions > 1)
  }

}
