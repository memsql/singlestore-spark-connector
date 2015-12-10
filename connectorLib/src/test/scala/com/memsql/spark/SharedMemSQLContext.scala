package com.memsql.spark

import java.net.InetAddress
import java.security.MessageDigest

import com.memsql.spark.connector.MemSQLConnectionPool
import com.memsql.spark.connector.util.JDBCImplicits._
import com.memsql.spark.connector.util.MemSQLConnectionInfo
import org.apache.spark.sql.memsql.MemSQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

///** Shares a local `MemSQLContext` between all tests in a suite and closes it at the end */
trait SharedMemSQLContext extends BeforeAndAfterAll {self: Suite =>

  val dbName: String = {
    // Generate a unique database name based on this machine
    // This is so that multiple people can run tests against
    // the same MemSQL cluster.
    val hostMD5 = MessageDigest.getInstance("md5").digest(
      InetAddress.getLocalHost.getAddress)
    "connector_tests_" + hostMD5.slice(0, 2).map("%02x".format(_)).mkString
  }

  val masterConnectionInfo: MemSQLConnectionInfo =
    MemSQLConnectionInfo("127.0.0.1", 3306, "root", "", dbName) // scalastyle:ignore

  val conf = new SparkConf()
    .setAppName("MemSQL Connector Test")
    .setMaster("local")
    .set("memsql.host", masterConnectionInfo.dbHost)
    .set("memsql.port", masterConnectionInfo.dbPort.toString)
    .set("memsql.user", masterConnectionInfo.user)
    .set("memsql.password", masterConnectionInfo.password)
    .set("memsql.defaultDatabase", masterConnectionInfo.dbName)

  val sc: SparkContext = new SparkContext(conf)
  val msc: MemSQLContext = new MemSQLContext(sc)

  override def beforeAll() {
    recreateDatabase
    super.beforeAll()
  }

  override def afterAll() {
    sc.stop()
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    super.afterAll()
  }

  def recreateDatabase: Unit = {
    MemSQLConnectionPool.withConnection(masterConnectionInfo.copy(dbName=""))(conn => {
      conn.withStatement(stmt => {
        stmt.execute("DROP DATABASE IF EXISTS " + dbName)
        stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName)
      })
    })
  }

  def sqlExec(query: String, params: Any*): Unit = {
    msc.getMemSQLCluster.withMasterConn(conn => {
      conn.withPreparedStatement(query, stmt => {
        stmt.fillParams(params)
        stmt.execute()
      })
    })
  }
}
