// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark.connector.sql

import com.memsql.spark.connector._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec

class TestConnCloseProviderSpec extends FlatSpec with SharedMemSQLContext{

  "TestConnCloseProviderSpec" should "close a connection" in {
    var expected = 0
    var bound = 5
    var count = 0

    // numConns can create a connection. Run it once to seed the connection. Then get the actual number.
    numConns()

    // Verify that closing a connection doesn't really close.
    this.withStatement(stmt => {
      stmt.execute("CREATE DATABASE IF NOT EXISTS x_db")

      expected = numConns()

      stmt.close()

      assert(expected == numConns())
    })

    assert(numConns() == expected)

    val rdd = sc.parallelize(Array(Row(1)))
    val schema = StructType(
      Array(
        StructField("a", IntegerType, true),
        StructField("b", BinaryType, false)))
    val df = ss.createDataFrame(rdd, StructType(Array(StructField("a", IntegerType, true))))
    count = numConns()
    assert(count < expected + bound && count >= expected)

    df.saveToMemSQL(dbName, "default_columns_table")
    count = numConns()
    assert(count < expected + bound && count >= expected)

    withStatement(stmt => {
      stmt.execute("CREATE TABLE a (a BIGINT AUTO_INCREMENT PRIMARY KEY, b BIGINT)")
      stmt.execute("INSERT INTO a (b) VALUES (1), (2), (3), (4), (5)")
      stmt.execute("CREATE TABLE b (c BIGINT AUTO_INCREMENT PRIMARY KEY, d BIGINT)")
      stmt.execute("INSERT INTO b (d) VALUES (1), (1), (2), (2), (3)")
    })
    count = numConns()
    assert(count < expected + bound && count >= expected)

    val a = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".a")))
      .load()

    val b = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".b")))
      .load()

    for (i <- 0 until 10) {
      a.collect()
      a.select("a").collect()
      a.select(a("a").as("test")).collect()
      a.select(a("a"), a("a")).collect()
      a.filter(a("b") > 3).collect()
      a.filter(a("b") > 3 && a("a") > 2).collect()
      a.orderBy(a("b").desc).collect()

      b.groupBy(b("d")).count().collect()
      b.groupBy(b("d")).agg(sum("c")).collect()

      a.select(a("a").as("foo"), a("b"))
        .filter(col("foo") > 2)
        .groupBy("b")
        .agg(sum("foo") as "bar")
        .orderBy("bar")
        .collect()

      count = numConns()
      assert(count < expected + bound && count >= expected)
    }
  }

  def numConns(): Int = {
    this.withStatement(stmt => {
      val result = stmt.executeQuery("SHOW STATUS LIKE 'THREADS_CONNECTED'")
      result.next()
      val conns = result.getString("Value").toInt
      //println("num conns = " + conns)

      val procResult = stmt.executeQuery("show processlist")
      //while (procResult.next()) {
      //println("\tprocesslist " + procResult.getString("Id") + " " + procResult.getString("db") + " " + procResult.getString("Command") + " " +
      //    procResult.getString("State") + " " + procResult.getString("Info"))
      //}
      conns
    })
  }

}
