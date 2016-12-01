package com.memsql.spark.connector.sql

import org.scalatest.{FlatSpec, Matchers}

class LucySpec extends FlatSpec with SharedMemSQLContext with Matchers {

  override def beforeAll(): Unit = {
    super.beforeAll()
    /*
    withStatement(stmt => {
      stmt.execute("CREATE TABLE t(a BIGINT PRIMARY KEY, t_FLOAT FLOAT, t_REAL REAL, t_DOUBLE DOUBLE)")
      stmt.execute("INSERT INTO t values (1, NULL, NULL, NULL)")
    })
    */
  }

  // TestTypes.scala checks that all types round-trip "null", i.e. scala null <--> SQL NULL.
  // Floating-point types can end up as NaN in some cases; this test double-checks their behavior.
  it should "just trying something" in {

    val noPushdownResult = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> ("db.geo")))
      .load()
      .collect()


    val test = "Hi"
  }
}
