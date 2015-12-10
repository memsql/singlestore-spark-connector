// scalastyle:off magic.number regex

package org.apache.spark.sql.memsql

import com.memsql.spark.SharedMemSQLContext
import org.apache.spark.sql.Row
import org.scalatest.{Matchers, FlatSpec}

class BugfixTests extends FlatSpec with SharedMemSQLContext with Matchers {

  def recreateSimpleTable: Unit = {
    sqlExec("DROP TABLE IF EXISTS foo")
    sqlExec("CREATE TABLE foo (a INT, SHARD())")
    for (i <- Range(0, 10)) {
      sqlExec("INSERT INTO foo (a) VALUES (?)", i)
    }
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

}
