// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark.connector.sql

import org.scalatest.{FlatSpec, Matchers}
import com.memsql.spark.connector.util.JDBCImplicits._

/**
  * Test that using a String-contains filter returns the correct result when the
  * search string includes "%" and "'", which are wildcards for MemSQL's LIKE operator
  */
class LikeWildcardEscapeSpec extends FlatSpec with SharedMemSQLContext with Matchers {
  override def beforeAll(): Unit = {
    super.beforeAll()
    this.withConnection(conn => {
      conn.withStatement(stmt => {
        stmt.execute("""
          CREATE TABLE wildcardEscape
          (id INT PRIMARY KEY, data VARCHAR(200), key(data))
        """)

        val insertValues = Range(0, 20)
          .map(i => s"""($i, '${"%02d".format(i)}% discount')""")
          .mkString(",")

        val apostropheValues = Range(20, 30)
          .map(i => s"""($i, 'user ${"%02d".format(i)}''s data')""")
          .mkString(",")

        val underscoreValues = Range(30, 40)
          .map(i => s"""($i, 'test_data_${"%02d".format(i)}')""")
          .mkString(",")

        stmt.execute("INSERT INTO wildcardEscape VALUES" + insertValues + "," + apostropheValues + "," + underscoreValues)
      })
    })
  }

  "StringContains filter" should "correctly escape '%' in SQL pushdown" in {
    val spark = ss
    import spark.implicits._

    val escapePercentResult = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".wildcardEscape")))
      .load().filter($"data".contains("10%"))
      .collect()

    assert(escapePercentResult.length === 1)
    assert(escapePercentResult(0)(1) === "10% discount")
  }

  it should "correctly escape single quote in SQL pushdown" in {
    val spark = ss
    import spark.implicits._

    val escapeQuoteResult = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".wildcardEscape")))
      .load().filter($"data".contains("25's"))
      .collect()

    assert(escapeQuoteResult.length === 1)
    assert(escapeQuoteResult(0)(1) === "user 25's data")
  }

  it should "correctly escape '_' in SQL pushdown" in {
    val spark = ss
    import spark.implicits._

    val escapeUnderscoreResult = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".wildcardEscape")))
      .load().filter($"data".contains("_31"))
      .collect()

    assert(escapeUnderscoreResult.length === 1)
    assert(escapeUnderscoreResult(0)(1) === "test_data_31")
  }
}
