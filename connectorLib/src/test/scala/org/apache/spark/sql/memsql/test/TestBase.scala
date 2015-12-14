package org.apache.spark.sql.memsql.test

import java.net.InetAddress
import java.security.MessageDigest
import java.sql.{PreparedStatement, Statement, Connection}

import com.memsql.spark.connector.{MemSQLContext, MemSQLConnectionPool}
import com.memsql.spark.connector.util.MemSQLConnectionInfo
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import com.memsql.spark.connector.util.JDBCImplicits._

trait TestBase {
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

  var sc: SparkContext = null
  var sqlContext: SQLContext = null
  var msc: MemSQLContext = null

  def sparkUp(local: Boolean=false): Unit = {
    recreateDatabase

    var conf = new SparkConf()
      .setAppName("MemSQL Connector Test")
      .set("memsql.host", masterConnectionInfo.dbHost)
      .set("memsql.port", masterConnectionInfo.dbPort.toString)
      .set("memsql.user", masterConnectionInfo.user)
      .set("memsql.password", masterConnectionInfo.password)
      .set("memsql.defaultDatabase", masterConnectionInfo.dbName)

    if (local) {
      conf = conf.setMaster("local")
    }

    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
    msc = new MemSQLContext(sc)
  }

  def sparkDown: Unit = {
    sc.stop()
    sc = null
    sqlContext = null
    msc = null
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  def withStatement[T](handle: Statement => T): T =
    withConnection(conn => conn.withStatement(handle))

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
