// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark

import org.apache.spark._
import org.apache.spark.sql.memsql.MemSQLContext


object TestConnCloseProvider {
  def main(args: Array[String]): Unit = new TestConnCloseProvider
}

class TestConnCloseProvider extends TestBase with Logging {
  def runTest(sc: SparkContext, msc: MemSQLContext): Unit = {
    var conns = 0

    this.withStatement(stmt => {
      stmt.execute("CREATE DATABASE IF NOT EXISTS x_db")

      println("sleeping for ten seconds while we let memsql set up the reference db")
      Thread.sleep(10000)

      conns = numConns()
      stmt.close()
      assert(conns == numConns())
    })

    assert(conns == numConns())
  }

  def numConns(): Int = {
    this.withStatement(stmt => {
      var result = stmt.executeQuery("SHOW STATUS LIKE 'THREADS_CONNECTED'")
      result.next()
      var conns = result.getString("Value").toInt
      println("num conns = " + conns)

      var procResult = stmt.executeQuery("show processlist")
      while (procResult.next()) {
        println("    processlist " + procResult.getString("Id") + " " + procResult.getString("db") + " " + procResult.getString("Command") + " " +
          procResult.getString("State") + " " + procResult.getString("Info"))
      }
      conns
    })
  }

}
