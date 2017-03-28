// scalastyle:off magic.number file.size.limit regex
package com.memsql.spark.connector.sql

import com.memsql.spark.connector._
import org.scalatest.{FlatSpec, Matchers}
import com.memsql.spark.connector.util.JDBCImplicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

/**
  * Test that changing the SparkSession RuntimeConfig settings for spark.memsql.host etc
  * affects the next attempt to connect to MemSQL.
  */
class ChangeSparkSessionConfSpec extends FlatSpec with SharedMemSQLContext with Matchers {
  override def beforeAll(): Unit = {
    super.beforeAll()
    this.withConnection(conn => {
      conn.withStatement(stmt => {
        stmt.execute("""
          CREATE TABLE t
          (id INT PRIMARY KEY, data VARCHAR(200), key(data))
        """)
      })
    })
  }

  "Changing the MemSQL settings in the SparkSession RuntimeConfig" should "be reflected in the next attempt to connect to MemSQL" in {
    val df = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> ("t")))
      .load()

    df.collect()

    // Change the configuration settings

    val newconf = new SparkConf()
      .setAppName("MemSQL Spark Connector Example")
      .set("spark.memsql.host", "fakehost")
      .set("spark.memsql.user", "fakeuser")
      .set("spark.memsql.password", "fakepassword")
      .set("spark.memsql.defaultDatabase", "fakedatabase")

    ss = SparkSession.builder().config(newconf).getOrCreate()

    assert(ss.conf.get("spark.memsql.host") == "fakehost")

    try {
      ss.getMemSQLCluster.withMasterConn[Boolean](conn => {
        conn.withStatement(stmt => {
          stmt.execute("""
          CREATE TABLE t
          (id INT PRIMARY KEY, data VARCHAR(200), key(data))
        """)
        })
      })
      assert(false, "The connection should have failed when using the new config settings")
    } catch {
      case e: java.sql.SQLException => {
        assert(e.getMessage.contains("Cannot create PoolableConnectionFactory"))
      }
    }

    try {
      val df2 = ss
        .read
        .format("com.memsql.spark.connector")
        .options(Map( "path" -> ("t")))
        .load()
      assert(false, "The connection should have failed when using the new config settings")
    } catch {
      case e: java.sql.SQLException => {
        assert(e.getMessage.contains("Cannot create PoolableConnectionFactory"))
      }
    }
  }
}
