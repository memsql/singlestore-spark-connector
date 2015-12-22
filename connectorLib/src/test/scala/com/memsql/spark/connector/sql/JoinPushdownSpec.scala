package com.memsql.spark.connector.sql

import org.apache.spark.sql.memsql.test.{SharedMemSQLContext, TestUtils}
import org.scalatest.{FlatSpec, Matchers}

class JoinPushdownSpec extends FlatSpec with SharedMemSQLContext with Matchers {

  override def beforeAll(): Unit = {
    super.beforeAll()
    withStatement(stmt => {
      stmt.execute("CREATE TABLE t(a bigint primary key, b bigint)")
      stmt.execute("CREATE TABLE s(c bigint primary key, d bigint)")
      stmt.execute("CREATE REFERENCE TABLE r(e bigint primary key, f bigint)")

      var insertQuery = ""
      for (i <- 0 until 999) {
        insertQuery = insertQuery + "(" + i + "," + i + "),"
      }
      insertQuery = insertQuery + "(1000,1000)"
      stmt.execute("INSERT INTO t values" + insertQuery)
      stmt.execute("INSERT INTO s values" + insertQuery)
      stmt.execute("INSERT INTO r values" + insertQuery)
    })
  }

  val pushedDownQueries = Array(
    "SELECT * FROM t JOIN s ON t.a = s.c",
    "SELECT * FROM t LEFT JOIN s ON t.a = s.c",
    "SELECT a, count(*) FROM t GROUP BY a",
    "SELECT s.c, sum(t.a) FROM t JOIN s on t.a = s.c GROUP BY s.c",
    "SELECT s.c, t.a, sum(t.b) FROM t JOIN s on t.a = s.c GROUP BY s.c, t.a",
    "SELECT s.c, t.b, sum(t.a) FROM t JOIN s on t.a = s.c GROUP BY s.c, t.b",
    "SELECT * FROM t JOIN r ON t.a = r.e",
    "SELECT * FROM t JOIN r ON t.a = r.f",
    "SELECT * FROM t JOIN r ON t.b = r.e",
    "SELECT * FROM t JOIN r ON t.b = r.f",
    "SELECT * FROM t JOIN s ON t.a = s.c WHERE c = 4",
    "SELECT * FROM t JOIN s ON t.a = s.c WHERE a = 4",
    "SELECT * FROM t LEFT JOIN s ON t.a = s.c WHERE c = 4",
    "SELECT * FROM t RIGHT JOIN s ON t.a = s.c WHERE c = 4",
    "SELECT * FROM t LEFT JOIN s ON t.a = s.c WHERE a = 4",
    "SELECT * FROM t RIGHT JOIN s ON t.a = s.c WHERE a = 4")

  val aggregatorQueries = Array(
    "SELECT * FROM t JOIN s ON t.a = s.d",
    "SELECT * FROM t JOIN s ON t.b = s.d",
    "SELECT * FROM t LEFT JOIN s ON t.a = s.d",
    "SELECT * FROM t LEFT JOIN s ON t.a = s.d",
    "SELECT * FROM t RIGHT JOIN s ON t.b = s.d",
    "SELECT b, count(*) FROM t GROUP BY b",
    "SELECT s.d, sum(t.a) FROM t JOIN s on t.a = s.c GROUP BY s.d",
    "SELECT s.d, t.b, sum(t.a) FROM t JOIN s on t.a = s.c GROUP BY s.d, t.b",
    "SELECT s.d, sum(t.a) FROM t JOIN s on t.a = s.c GROUP BY s.d",
    "SELECT e, count(*) FROM t JOIN r ON t.a = r.e GROUP BY r.e",
    "SELECT f, count(*) FROM t JOIN r ON t.a = r.e GROUP BY r.f",
    "SELECT e, count(*) FROM t JOIN r ON t.a = r.f GROUP BY r.e",
    "SELECT f, count(*) FROM t JOIN r ON t.a = r.f GROUP BY r.f")

  "MemSQLRDD" should "push down simple joins" in {
    pushedDownQueries.foreach(q => {
      val df = msc.sql(q, true)

      // These queries are simple enough to be pushed down to independent operations on MemSQL leaf nodes, and we expect
      // to get back a Spark RDD for each MemSQL partition.
      assert(df.rdd.partitions.size > 1, q)

      // The limit prevents pushdown. We compare the results to test correctness.
      val compare_df = msc.sql(q + " LIMIT 999999999", true)
      assert(compare_df.rdd.partitions.size == 1, q)
      assert(TestUtils.equalDFs(df, compare_df), q)
    })
  }

  "MemSQLRDD" should "not push down distributed joins" in {
    aggregatorQueries.foreach(q => {
      val df = msc.sql(q, true)

      // These queries must be handled by the aggregator nodes.
      assert(df.rdd.partitions.size == 1, q)
    })
  }
}
