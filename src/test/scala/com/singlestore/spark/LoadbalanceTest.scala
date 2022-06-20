package com.singlestore.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.singlestore.spark.JdbcHelpers.executeQuery
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types.IntegerType

class LoadbalanceTest extends IntegrationSuiteBase {

  val masterHostPort = s"${masterHost}:${masterPort}"
  val childHostPort  = "localhost:5508"

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Set master + child aggregator as dmlEndpoints
    spark.conf
      .set("spark.datasource.singlestore.dmlEndpoints", s"${masterHostPort},${childHostPort}")
  }

  def countQueries(hostport: String): Int = {
    val opts = new JDBCOptions(
      Map("url"      -> s"jdbc:singlestore://$hostport",
          "dbtable"  -> "testdb",
          "user"     -> "root",
          "password" -> masterPassword))

    JdbcUtils.withConnection(opts)(conn => {
      // we only use write queries since read queries are always increasing due to internal status checks
      val rows =
        JdbcHelpers.executeQuery(conn, "show status extended like 'Successful_write_queries'")
      rows.map(r => r.getAs[String](1).toInt).sum
    })
  }

  def counters =
    Map(
      masterHostPort -> countQueries(masterHostPort),
      childHostPort  -> countQueries(childHostPort)
    )

  describe("load-balances among all hosts listed in dmlEndpoints") {

    it("queries both aggregators eventually") {

      val df = spark.createDF(
        List(4, 5, 6),
        List(("id", IntegerType, true))
      )

      val startCounters = counters

      // 50/50 chance of picking either agg, 10 tries should be enough to ensure we hit both aggs with write queries
      for (i <- 0 to 10) {
        df.write
          .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
          .option("driverConnectionPool.MinEvictableIdleTimeMs", "100")
          .option("driverConnectionPool.TimeBetweenEvictionRunsMS", "50")
          .option("executorConnectionPool.MinEvictableIdleTimeMs", "100")
          .option("executorConnectionPool.TimeBetweenEvictionRunsMS", "50")
          .mode(SaveMode.Overwrite)
          .save("test")

        Thread.sleep(300)
      }

      val endCounters = counters

      assert(endCounters(childHostPort) > startCounters(childHostPort))
      assert(endCounters(masterHostPort) > startCounters(masterHostPort))
    }

  }
}
