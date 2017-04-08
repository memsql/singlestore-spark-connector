// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark.connector.sql

import org.scalatest.FlatSpec
import com.memsql.spark.connector.util.JDBCImplicits._
import java.sql.ResultSet

/**
  * Test that Spark filters applied to MemSQL dataframes get pushed down to
  * the SQL query that is sent to MemSQL
  */
class FilterPushdownSpec extends FlatSpec with SharedMemSQLContext{
  override def beforeAll(): Unit = {
    super.beforeAll()
    this.withConnection(conn => {
      conn.withStatement(stmt => {
        stmt.execute("""
          CREATE TABLE pushdown
          (id INT PRIMARY KEY, data VARCHAR(200), `col#int` INTEGER, `col#str` VARCHAR(200), key(data))
        """)

        val insertValues = Range(0, 20)
          .map(i => s"""($i, 'test_data_${"%02d".format(i)}', $i, 'special_char_col_${"%02d".format(i)}')""")
          .mkString(",")

        stmt.execute("INSERT INTO pushdown VALUES" + insertValues)
      })
    })
  }

  "Evaluating dataframes from Spark datasources API" should "pushdown one filter to MemSQL" in {
    val pushdownDataframe = ss
          .read
          .format("com.memsql.spark.connector")
          .options(Map( "path" -> (dbName + ".pushdown")))
          .load()
          .filter("id = 2")

    val result = pushdownDataframe.collect()

    // Check the SQL queries in the plancache of a leaf node
    withStatement(leafConnectionInfo)(stmt => {
      val plancacheRaw: ResultSet = stmt.executeQuery("show plancache")

      // Get only the queries for the test database
      val plancache = plancacheRaw.toIterator.filter(_.getString("Database") contains dbName).map(_.getString("QueryText"))

      // Verify there is a query that contains the equality filter
      val whereExists = plancache.filter(x => (x.contains("WHERE") && x.contains("id") && x.contains("=")))
      assert(whereExists.length > 0)
    })

    // Verify correctness of data -- that there is only 1 and its id is 2
    assert(result.length == 1)
    assert(result(0)(0) == 2)
  }

  it should "pushdown filters to MemSQL" in {
    // import implicits to make $ notation work in filters
    val spark = ss
    import spark.implicits._

    val pushdownDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".pushdown")))
      .load()
      .filter("id > 2")
      .filter("`col#int` = 14")
      .filter($"data".contains("14"))
      .filter($"col#str".contains("special_char_col_14"))

    val result = pushdownDataframe.collect()

    // check the SQL queries in the plancache in the leaves
    withStatement(leafConnectionInfo)(stmt => {
      val plancacheRaw: ResultSet = stmt.executeQuery("show plancache")
      val plancache = plancacheRaw.toIterator.filter(_.getString("Database") contains dbName).map(_.getString("QueryText"))

      // Verify there is a query that contains the greater-than and string-contains filter
      val whereExists = plancache.filter(x => (x.contains("WHERE")
        && x.contains("id")
        && x.contains(">")
        && x.contains("AND")
        && x.contains("data")
        && x.contains("LIKE")))

      assert(whereExists.length > 0)
    })

    // Verify correctness of data -- that there is only 1 and its data is "test_data_14"
    assert(result.length==1)
    assert(result(0)(1) == "test_data_14")
  }
}
