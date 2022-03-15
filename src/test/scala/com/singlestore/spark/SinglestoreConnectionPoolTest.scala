package com.singlestore.spark

import java.sql.Connection
import java.time.Duration
import java.util.Properties

import org.apache.commons.dbcp2.DelegatingConnection
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class SinglestoreConnectionPoolTest extends IntegrationSuiteBase {
  var properties = new Properties()

  override def beforeEach(): Unit = {
    super.beforeEach()
    properties = JdbcHelpers.getConnProperties(
      SinglestoreOptions(
        CaseInsensitiveMap(
          Map("ddlEndpoint" -> s"$clusterHost:$adminPort", "password" -> masterPassword))),
      isOnExecutor = false,
      s"$clusterHost:$adminPort"
    )
  }

  override def afterEach(): Unit = {
    super.afterEach()
    SinglestoreConnectionPool.close()
  }

  it("reuses a connection") {
    var conn  = SinglestoreConnectionPool.getConnection(properties)
    val conn1 = conn.asInstanceOf[DelegatingConnection[Connection]].getInnermostDelegateInternal
    conn.close()

    conn = SinglestoreConnectionPool.getConnection(properties)
    val conn2 = conn.asInstanceOf[DelegatingConnection[Connection]].getInnermostDelegateInternal
    conn.close()

    assert(conn1 == conn2, "should reuse idle connection")
  }

  it("creates a new connection when existing is in use") {
    val conn1 = SinglestoreConnectionPool.getConnection(properties)
    val conn2 = SinglestoreConnectionPool.getConnection(properties)
    val originalConn1 =
      conn1.asInstanceOf[DelegatingConnection[Connection]].getInnermostDelegateInternal
    val originalConn2 =
      conn2.asInstanceOf[DelegatingConnection[Connection]].getInnermostDelegateInternal

    assert(originalConn1 != originalConn2, "should create a new connection when existing is in use")

    conn1.close()
    conn2.close()
  }

  it("creates different pools for different properties") {
    var conn  = SinglestoreConnectionPool.getConnection(properties)
    val conn1 = conn.asInstanceOf[DelegatingConnection[Connection]].getInnermostDelegateInternal
    conn.close()

    properties.setProperty("newProperty", "")

    conn = SinglestoreConnectionPool.getConnection(properties)
    val conn2 = conn.asInstanceOf[DelegatingConnection[Connection]].getInnermostDelegateInternal
    conn.close()

    assert(conn1 != conn2, "should create different pools for different properties")
  }

  it("maxTotal and maxWaitMillis") {
    val maxWaitMillis = 200
    properties.setProperty("maxTotal", "1")
    properties.setProperty("maxWaitMillis", maxWaitMillis.toString)

    val conn1 = SinglestoreConnectionPool.getConnection(properties)

    val start = System.nanoTime()
    try {
      SinglestoreConnectionPool.getConnection(properties)
      fail()
    } catch {
      case e: Throwable =>
        assert(
          e.getMessage.equals(
            "Cannot get a connection, pool error Timeout waiting for idle object"),
          "should throw timeout error"
        )
        assert(System.nanoTime() - start > Duration.ofMillis(maxWaitMillis).toNanos,
               "should throw timeout error after 1 sec")
    }

    conn1.close()
  }

  it("eviction of idle connections") {
    val minEvictableIdleTimeMillis    = 100
    val timeBetweenEvictionRunsMillis = 50
    properties.setProperty("minEvictableIdleTimeMillis", minEvictableIdleTimeMillis.toString)
    properties.setProperty("timeBetweenEvictionRunsMillis", timeBetweenEvictionRunsMillis.toString)

    var conn  = SinglestoreConnectionPool.getConnection(properties)
    val conn1 = conn.asInstanceOf[DelegatingConnection[Connection]].getInnermostDelegateInternal
    conn.close()

    Thread.sleep(((minEvictableIdleTimeMillis + timeBetweenEvictionRunsMillis) * 1.1).toLong)

    conn = SinglestoreConnectionPool.getConnection(properties)
    val conn2 = conn.asInstanceOf[DelegatingConnection[Connection]].getInnermostDelegateInternal
    conn.close()

    assert(conn1 != conn2, "should evict idle connection")
  }

  it("maxConnLifetimeMillis") {
    val maxConnLifetimeMillis = 100
    properties.setProperty("maxConnLifetimeMillis", maxConnLifetimeMillis.toString)
    var conn  = SinglestoreConnectionPool.getConnection(properties)
    val conn1 = conn.asInstanceOf[DelegatingConnection[Connection]].getInnermostDelegateInternal
    conn.close()

    Thread.sleep((maxConnLifetimeMillis * 1.1).toLong)

    conn = SinglestoreConnectionPool.getConnection(properties)
    val conn2 = conn.asInstanceOf[DelegatingConnection[Connection]].getInnermostDelegateInternal
    conn.close()

    assert(conn1 != conn2, "should not use a connection after end of lifetime")
  }
}
