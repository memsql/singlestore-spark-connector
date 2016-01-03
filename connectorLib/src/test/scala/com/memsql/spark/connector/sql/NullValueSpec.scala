package com.memsql.spark.connector.sql

import com.memsql.spark.connector.MemSQLContext
import com.memsql.spark.pushdown.MemSQLPushdownStrategy
import org.apache.spark.sql.memsql.test.SharedMemSQLContext
import org.scalatest.{FlatSpec, Matchers}

class NullValueSpec extends FlatSpec with SharedMemSQLContext with Matchers {

  override def beforeAll(): Unit = {
    super.beforeAll()
    withStatement(stmt => {
      stmt.execute("CREATE TABLE t(a BIGINT PRIMARY KEY, t_FLOAT FLOAT, t_REAL REAL, t_DOUBLE DOUBLE)")
      stmt.execute("INSERT INTO t values (1, NULL, NULL, NULL)")
    })
  }

  // TestTypes.scala checks that all types round-trip "null", i.e. scala null <--> SQL NULL.
  // Floating-point types can end up as NaN in some cases; this test double-checks their behavior.
  it should "read NULL values the same with or without pushdown, for FLOAT, REAL, and DOUBLE types" in {
    val mscNoPushdown = new MemSQLContext(sc)
    MemSQLPushdownStrategy.unpatchSQLContext(mscNoPushdown)

    val pushdownResult = msc.table("t").collect()(0)
    val noPushdownResult = mscNoPushdown.table("t").collect()(0)

    assert(pushdownResult.isNullAt(1))
    assert(pushdownResult.isNullAt(2))
    assert(pushdownResult.isNullAt(3))

    assert(noPushdownResult.isNullAt(1))
    assert(noPushdownResult.isNullAt(2))
    assert(noPushdownResult.isNullAt(3))
  }
}
