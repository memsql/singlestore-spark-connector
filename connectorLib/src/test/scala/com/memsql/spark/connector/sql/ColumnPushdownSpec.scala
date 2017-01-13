// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark.connector.sql

import java.sql.ResultSet

import org.scalatest.FlatSpec
import com.memsql.spark.connector.util.JDBCImplicits._

/**
  * Test that Spark select operations applied to MemSQL dataframes get pushed down to
  * the SQL query that is sent to MemSQL
  */
class ColumnPushdownSpec extends FlatSpec with SharedMemSQLContext{
  override def beforeAll(): Unit = {
    super.beforeAll()
    this.withConnection(conn => {
      conn.withStatement(stmt => {
        stmt.execute("""
          CREATE TABLE columnpushdown
          (id INT PRIMARY KEY, columna VARCHAR(200), columnb VARCHAR(200), key(id))
        """)

        val insertValues = Range(0, 40)
          .map(i => s"""($i, 'a_${"%02d".format(i)}', 'b_${"%02d".format(i)}')""")
          .mkString(",")

        stmt.execute("INSERT INTO columnpushdown VALUES" + insertValues)
      })
    })
  }

  "Evaluating dataframes from Spark datasources API" should "pushdown column selection to MemSQL" in {
    val pushdownDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".columnpushdown")))
      .load()
      .select("columna")

    val result = pushdownDataframe.collect()
      .map(x => {
        assert(x.length === 1)  //there should only be one column: "columna"
        x.getString(0)
      })
      .sortWith(_ < _)

    // Check the SQL queries in the plancache of a leaf node
    withStatement(leafConnectionInfo)(stmt => {
      val plancacheRaw: ResultSet = stmt.executeQuery("show plancache")

      // Get only the queries for the test database
      val plancache = plancacheRaw.toIterator.filter(_.getString("Database") contains dbName).map(_.getString("QueryText")).toArray

      // Verify there is a query that selects 'columna' but not 'id' or 'columnb'
      val whereExists = plancache.filter(x => (x.contains("SELECT")
        && x.contains("columnpushdown")
        && x.contains("columna")
        && !x.contains("id")
        && !x.contains("columnb")))
      assert(whereExists.length > 0)
    })

    // Verify dataframe schema
    assert(pushdownDataframe.columns.length === 1)
    assert(pushdownDataframe.columns(0).equals("columna"))

    // Check some of the data
    assert(result(0) === "a_00")
    assert(result(1) === "a_01")
    assert(result(2) === "a_02")
  }
}
