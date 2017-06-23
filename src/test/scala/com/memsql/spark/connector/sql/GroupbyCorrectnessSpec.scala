// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark.connector.sql

import org.scalatest.FlatSpec
import com.memsql.spark.connector.util.JDBCImplicits._

/**
  * Tests that a query with a groupby is not incorrectly pushed down to leaves
  */
class GroupbyCorrectnessSpec extends FlatSpec with SharedMemSQLContext{
  override def beforeAll(): Unit = {
    super.beforeAll()
    this.withConnection(conn => {
      conn.withStatement(stmt => {
        stmt.execute("CREATE TABLE groupbycorrectness (id INT, data VARCHAR(32), PRIMARY KEY (id))")

        stmt.execute("""INSERT INTO groupbycorrectness(id, data) VALUES
          (1, "a"),(2, "b"),(3, "c"),(4, "d"),(5, "e"),(6, "a"),(7, "b"),(8, "c"),(9, "e"),(10, "b"),
          (11, "a"),(12, "b"),(13, "c"),(14, "d"),(15, "e"),(16, "a"),(17, "b"),(18, "c"),(19, "e"),(20, "b"),
          (21, "a"),(22, "b"),(23, "c"),(24, "d"),(25, "e"),(26, "a"),(27, "b"),(28, "c"),(29, "e"),(30, "b")""")
      })
    })
  }

  "A query with groupby" should "not push the query down to the leaves" in {
    val groupbyDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "query" -> s"select data,count(*) from ${dbName}.groupbycorrectness group by data"))
      .load()

    assert(groupbyDataframe.rdd.getNumPartitions == 1)

    val data = groupbyDataframe.collect().map(x => s"${x.getString(0)}${x.getLong(1)}").sortWith(_ < _)
    assert(data.length === 5)
    assert(data(0).equals("a6"))
    assert(data(1).equals("b9"))
    assert(data(2).equals("c6"))
    assert(data(3).equals("d3"))
    assert(data(4).equals("e6"))
  }
}
