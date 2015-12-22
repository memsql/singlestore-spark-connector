package com.memsql.spark.connector.sql

import java.sql.SQLException

import com.memsql.spark.connector.util.JDBCImplicits.ConnectionHelpers
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException
import org.apache.spark.sql.memsql.test.{SharedMemSQLContext, TestUtils}
import org.scalatest.{FlatSpec, Matchers}

class PartitionPushdownSpec extends FlatSpec with SharedMemSQLContext with Matchers {

  val otherDbName = dbName + "_other"

  override def beforeAll(): Unit = {
    super.beforeAll()

    var insertQuery = ""
    for (i <- 0 until 999) {
      insertQuery = insertQuery + s"(${i}, ${i}),"
    }
    insertQuery = insertQuery + "(999, 999)"

    withStatement(stmt => {
      stmt.execute("CREATE TABLE x(a BIGINT PRIMARY KEY, b BIGINT)")
      stmt.execute("CREATE TABLE y(c BIGINT PRIMARY KEY, d BIGINT)")
      stmt.execute("INSERT INTO x VALUES" + insertQuery)
      stmt.execute("INSERT INTO y VALUES" + insertQuery)
    })

    withConnection(masterConnectionInfo.copy(dbName=""))(conn => {
      conn.withStatement(stmt => {
        stmt.execute("DROP DATABASE IF EXISTS " + otherDbName)
        stmt.execute("CREATE DATABASE IF NOT EXISTS " + otherDbName)
      })
    })
    withConnection(masterConnectionInfo.copy(dbName=otherDbName))(conn => {
      conn.withStatement(stmt => {
        stmt.execute("CREATE TABLE z(e BIGINT PRIMARY KEY, f BIGINT)")
        stmt.execute("CREATE REFERENCE TABLE r(g BIGINT PRIMARY KEY, h BIGINT)")
        stmt.execute("INSERT INTO z VALUES" + insertQuery)
        stmt.execute("INSERT INTO r VALUES" + insertQuery)
      })
    })
  }

  def assertPushdown(q: String): Unit = {
    val df = msc.sql(q, true)
    assert(df.rdd.partitions.size > 1, q)

    // The limit prevents pushdown. We compare the results to test correctness.
    val compare_df = msc.sql(q + " LIMIT 999999999", true)
    assert(compare_df.rdd.partitions.size == 1, q)
    assert(TestUtils.equalDFs(df, compare_df), q)
  }

  def assertNoPushdown(q: String): Unit = {
    val df = msc.sql(q, true)
    assert(df.rdd.partitions.size == 1, q)
  }

  "MemSQLRDD" should "pushdown simple queries" in {
    assertPushdown("SELECT * FROM x")
    assertPushdown(s"SELECT * FROM ${dbName}.x")
    assertPushdown("SELECT a FROM x")
    assertPushdown("SELECT a AS blah FROM x")
    assertPushdown("SELECT * FROM x WHERE a > 17")
    assertPushdown("SELECT * FROM x WHERE a > 17 AND b < 42")
    assertPushdown("SELECT a FROM x GROUP BY a")
    assertPushdown("SELECT a FROM x GROUP BY a, b")
    assertPushdown("SELECT count(*) FROM x GROUP BY a")
    assertPushdown("SELECT sum(b) FROM x GROUP BY a")
  }

  "MemSQLRDD" should "pushdown joins to reference tables in other databases" in {
    assertPushdown(s"SELECT * FROM ${dbName}.x x JOIN ${otherDbName}.r r ON x.a = r.g")
  }

  "MemSQLRDD" should "not pushdown unions" in {
    assertNoPushdown("SELECT * FROM x UNION SELECT * FROM y")
  }

  it should "fail multi-database distributed joins" in {
    a[SQLException] should be thrownBy {
      assertNoPushdown(s"SELECT * FROM ${dbName}.x x JOIN ${otherDbName}.z z ON x.a = z.e")
    }
  }

  it should "fail 'insert select'" in {
    a[MySQLSyntaxErrorException] should be thrownBy {
      assertNoPushdown("INSERT INTO x(a, b) SELECT c + 1000, d FROM y")
    }
  }
}
