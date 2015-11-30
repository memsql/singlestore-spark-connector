package com.memsql.spark

import com.memsql.spark.pushdown.MemSQLPushdownStrategy
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.memsql.MemSQLContext
import org.apache.spark.sql.functions._

object TestMemSQLQueryPushdown {
  def main(args: Array[String]): Unit = new TestMemSQLQueryPushdown
}

class TestMemSQLQueryPushdown extends TestBase with Logging {
  type DFTuple = (DataFrame, DataFrame)

  def runTest(sc: SparkContext, msc: MemSQLContext): Unit = {
    val mscNoPushdown = new MemSQLContext(sc)
    MemSQLPushdownStrategy.unpatchSQLContext(mscNoPushdown)

    withStatement(stmt => {
      stmt.execute("CREATE TABLE a (a BIGINT AUTO_INCREMENT PRIMARY KEY, b BIGINT)")
      stmt.execute("INSERT INTO a (b) VALUES (1), (2), (3), (4), (5)")
      stmt.execute("CREATE TABLE b (c BIGINT AUTO_INCREMENT PRIMARY KEY, d BIGINT)")
      stmt.execute("INSERT INTO b (d) VALUES (1), (1), (2), (2), (3)")
    })

    val allDFs = (
      (mscNoPushdown.table("a"), mscNoPushdown.table("b")),
      (msc.table("a"),           msc.table("b"))
    )

    TestUtils.runQueries[DFTuple](allDFs, {
      case (a: DataFrame, b: DataFrame) => {
        Seq(
          a.select("a"),
          a.select(a("a").as("test")),
          a.select(a("a"), a("a")),
          a.filter(a("b") > 3),
          a.filter(a("b") > 3 && a("a") > 2),
          a.orderBy(a("b").desc),

          b.groupBy(b("d")).count(),
          b.groupBy(b("d")).agg(sum("c")),

          a.select(a("a").as("foo"), a("b"))
            .filter(col("foo") > 2)
            .groupBy("b")
            .agg(sum("foo") as "bar")
            .orderBy("bar"),

          a.join(b),
          a.join(b, a("a") === b("c")),
          a.join(b, a("a") === b("d")),

          a.select(a("a"), a("a")).join(b),

          a.filter(a("a") !== 1)
            .orderBy(desc("b"))
            .join(
              b.groupBy("d").agg(sum("c") as "sum_of_c"),
              a("b") === col("sum_of_c")
            )
        )
      }
    })
  }
}
