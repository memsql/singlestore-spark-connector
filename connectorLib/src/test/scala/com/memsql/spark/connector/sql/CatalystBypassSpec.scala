// scalastyle:off regex

package com.memsql.spark.connector.sql

import org.apache.spark.sql.memsql.test.SharedMemSQLContext
import org.scalatest.{Matchers, FlatSpec}

class CatalystBypassSpec extends FlatSpec with SharedMemSQLContext with Matchers {

  override def beforeAll: Unit = {
    super.beforeAll
    withStatement(stmt => {
      stmt.execute("CREATE TABLE t (a BIGINT AUTO_INCREMENT PRIMARY KEY, b BIGINT)")
      stmt.execute("INSERT INTO t (b) VALUES (1), (2), (3), (4), (5)")
      stmt.execute("CREATE TABLE s (c BIGINT AUTO_INCREMENT PRIMARY KEY, d BIGINT)")
      stmt.execute("INSERT INTO s (d) VALUES (1), (1), (2), (2), (3)")
    })
  }

  val queries = Seq(
    "SELECT a FROM t",
    "SELECT b FROM t",
    "SELECT a AS test FROM t",
    "SELECT b FROM t WHERE b > 3",
    "SELECT a FROM t WHERE b > 3",
    "SELECT b FROM t WHERE b > 3 AND a > 2",
    "SELECT max(c) FROM s GROUP BY d",
    "SELECT max(foo) AS bar FROM (SELECT a AS foo, b FROM t WHERE a > 2) x GROUP BY b ORDER BY bar"
  )

  val orderByQueries = Seq(
    "SELECT a FROM t ORDER BY b",
    "SELECT a FROM t ORDER BY b ASC",
    "SELECT a FROM t ORDER BY b DESC",
    "SELECT d AS a FROM s ORDER BY c",
    "SELECT d AS a FROM s ORDER BY c ASC",
    "SELECT d AS a FROM s ORDER BY c DESC"
  )

  it should "be the same result with and without bypass" in {
    queries.foreach(q => {
      val df1 = msc.sql(q)
      val arr1 = df1.collect.map(_.getLong(0))

      val df2 = msc.sql(q, true)
      val arr2 = df2.collect.map(_.getLong(0))

      arr1 should contain theSameElementsAs arr2.sorted
    })
  }

  it should "handle ORDER BY with Spark (not MemSQL) semantics" in {
    orderByQueries.foreach(q => {
      val df1 = msc.sql(q)
      val arr1 = df1.collect.map(_.getLong(0))

      val df2 = msc.sql(q, true)
      val arr2 = df2.collect.map(_.getLong(0))

      arr1 should contain theSameElementsInOrderAs arr2

      val df3 = df1.filter(df1("a") > 1)
      val arr3 = df3.collect.map(_.getLong(0))

      val df4 = df2.filter(df2("a") > 1)
      val arr4 = df4.collect.map(_.getLong(0))

      arr1 should contain theSameElementsInOrderAs arr2
    })
  }
}
