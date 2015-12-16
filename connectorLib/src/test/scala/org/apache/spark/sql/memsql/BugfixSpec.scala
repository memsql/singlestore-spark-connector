// scalastyle:off magic.number regex

package org.apache.spark.sql.memsql

import com.memsql.spark.pushdown.MemSQLPushdownStrategy
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.memsql.test.SharedMemSQLContext
import org.scalatest.{Matchers, FlatSpec}

class BugfixSpec extends FlatSpec with SharedMemSQLContext with Matchers {

  def recreateSimpleTable: Unit = {
    sqlExec("DROP TABLE IF EXISTS foo")
    sqlExec("CREATE TABLE foo (a INT, SHARD())")
    for (i <- Range(0, 10)) {
      sqlExec("INSERT INTO foo (a) VALUES (?)", i)
    }
  }

  def getPhysicalPlan(query: String): Seq[SparkPlan] = {
    val df = msc.sql(query)
    val plan = df.queryExecution.optimizedPlan
    val strategy = new MemSQLPushdownStrategy(sc)
    strategy(plan)
  }

  "getQuerySchema" should "lock out duplicate columns" in {
    recreateSimpleTable
    intercept[UnsupportedOperationException] {
      msc.getMemSQLCluster.getQuerySchema("select a, a from foo")
    }
  }

  "PushdownStrategy" should "support duplicate columns projected from two Join queries" in {
    recreateSimpleTable
    val join = msc.sql("select * from foo x inner join foo y on x.a = y.a order by x.a")
    val expected = Range(0, 10).map(i => Row(i, i)).toArray
    join.collect should equal (expected)
  }

  "PushdownStrategy" should "support plans with empty aggregates" in {
    recreateSimpleTable
    val plan = getPhysicalPlan("SELECT COUNT(*) FROM (SELECT COUNT(*) FROM foo) x")
    plan should not be (Nil)
  }

  "PushdownStrategy" should "support plans with limits and orders" in {
    recreateSimpleTable
    val plan = getPhysicalPlan("SELECT * FROM foo order by a limit 5")
    plan should not be (Nil)
  }

  "PushdownStrategy" should "support plans with just orders" in {
    recreateSimpleTable
    val plan = getPhysicalPlan("SELECT * FROM foo order by a")
    plan should not be (Nil)
  }
}
