package com.memsql.spark.connector.sql

import java.net.InetAddress
import java.security.MessageDigest
import java.sql.{Connection, PreparedStatement, Statement}

import com.memsql.spark.connector.{MemSQLCluster, MemSQLConnectionPool}
import com.memsql.spark.connector.util.JDBCImplicits._
import com.memsql.spark.connector.util.MemSQLConnectionInfo
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

trait TestBase {
  val dbName: String = {
    // Generate a unique database name based on this machine
    // This is so that multiple people can run tests against
    // the same MemSQL cluster.
    val hostMD5 = MessageDigest.getInstance("md5").digest(
      InetAddress.getLocalHost.getAddress)
    "connector_tests_" + hostMD5.slice(0, 2).map("%02x".format(_)).mkString
  }

  val masterHost = sys.env.get("MEMSQL_HOST_TEST").getOrElse("172.17.0.4")
  val masterConnectionInfo: MemSQLConnectionInfo =
    MemSQLConnectionInfo(masterHost, 3306, "root", "", dbName) // scalastyle:ignore
  val leafConnectionInfo: MemSQLConnectionInfo =
    MemSQLConnectionInfo(masterHost, 3307, "root", "", dbName) // scalastyle:ignore

  var ss: SparkSession = null
  var sc: SparkContext = null

  def sparkUp(local: Boolean=false): Unit = {
    recreateDatabase

    var conf = new SparkConf()
      .setAppName("MemSQL Connector Test")
      .set("spark.memsql.host", masterConnectionInfo.dbHost)
      .set("spark.memsql.port", masterConnectionInfo.dbPort.toString)
      .set("spark.memsql.user", masterConnectionInfo.user)
      .set("spark.memsql.password", masterConnectionInfo.password)
      .set("spark.memsql.defaultDatabase", masterConnectionInfo.dbName)

    if (local) {
      conf = conf.setMaster("local")
    }

    ss = SparkSession.builder().config(conf).getOrCreate()
    sc = ss.sparkContext
  }

  def sparkDown: Unit = {
    ss.stop()
    sc.stop()
    ss = null
    sc = null
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  def withStatement[T](handle: Statement => T): T =
    withConnection(conn => conn.withStatement(handle))

  def withStatement[T](info: MemSQLConnectionInfo)(handle: Statement => T): T =
    withConnection(info)(conn => conn.withStatement(handle))

  def withPreparedStatement[T](query: String, handle: PreparedStatement => T): T =
    withConnection(conn => conn.withPreparedStatement(query, handle))

  def withConnection[T](handle: Connection => T): T =
    withConnection[T](masterConnectionInfo)(handle)

  def withConnection[T](info: MemSQLConnectionInfo)(handle: Connection => T): T =
    MemSQLConnectionPool.withConnection(info)(handle)

  def recreateDatabase: Unit = {
    withConnection(masterConnectionInfo.copy(dbName=""))(conn => {
      conn.withStatement(stmt => {
        stmt.execute("DROP DATABASE IF EXISTS " + dbName)
        stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName)
      })
    })
  }

  def sqlExec(query: String, params: Any*): Unit = {
    withPreparedStatement(query, stmt => {
      stmt.fillParams(params)
      stmt.execute()
    })
  }
}
