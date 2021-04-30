package com.singlestore.spark

import java.sql.{PreparedStatement, SQLException}

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.singlestore.spark.SQLGen.VariableList
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class ExternalHostTest
    extends IntegrationSuiteBase
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with MockitoSugar {

  val testDb         = "testdb"
  val testCollection = "externalHost"

  def setupMockJdbcHelper(): Unit = {
    when(JdbcHelpers.loadSchema(any[SinglestoreOptions], any[String], any[SQLGen.VariableList]))
      .thenCallRealMethod()
    when(JdbcHelpers.getDDLJDBCOptions(any[SinglestoreOptions])).thenCallRealMethod()
    when(JdbcHelpers.getDMLJDBCOptions(any[SinglestoreOptions])).thenCallRealMethod()
    when(JdbcHelpers.getJDBCOptions(any[SinglestoreOptions], any[String])).thenCallRealMethod()
    when(JdbcHelpers.explainJSONQuery(any[SinglestoreOptions], any[String], any[VariableList]))
      .thenCallRealMethod()
    when(JdbcHelpers.partitionHostPorts(any[SinglestoreOptions], any[String]))
      .thenCallRealMethod()
    when(JdbcHelpers.fillStatement(any[PreparedStatement], any[VariableList]))
      .thenCallRealMethod()
  }

  describe("success tests") {
    it("low SingleStore version") {
      val df = spark.createDF(
        List((2, "B")),
        List(("id", IntegerType, true), ("name", StringType, true))
      )
      writeTable(s"$testDb.$testCollection", df)

      withObjectMocked[JdbcHelpers.type] {

        setupMockJdbcHelper()
        when(JdbcHelpers.getSinglestoreVersion(any[SinglestoreOptions])).thenReturn("6.8.10")

        val actualDF =
          spark.read
            .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
            .option("useExternalHost", "true")
            .load(s"$testDb.$testCollection")

        assertSmallDataFrameEquality(
          actualDF,
          df
        )
      }
    }

    it("valid external host") {
      val df = spark.createDF(
        List((2, "B")),
        List(("id", IntegerType, true), ("name", StringType, true))
      )
      writeTable(s"$testDb.$testCollection", df)

      withObjectMocked[JdbcHelpers.type] {

        setupMockJdbcHelper()
        when(JdbcHelpers.getSinglestoreVersion(any[SinglestoreOptions])).thenReturn("7.1.0")

        val externalHostMap = Map(
          "172.17.0.2:3307" -> "172.17.0.2:3307"
        )
        when(JdbcHelpers.externalHostPorts(any[SinglestoreOptions]))
          .thenReturn(externalHostMap)

        val actualDF =
          spark.read
            .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
            .option("useExternalHost", "true")
            .load(s"$testDb.$testCollection")

        assertSmallDataFrameEquality(
          actualDF,
          df
        )
      }
    }
  }

  describe("failed tests") {
    it("empty external host map") {
      val df = spark.createDF(
        List((2, "B")),
        List(("id", IntegerType, true), ("name", StringType, true))
      )
      writeTable(s"$testDb.$testCollection", df)

      withObjectMocked[JdbcHelpers.type] {

        setupMockJdbcHelper()
        when(JdbcHelpers.getSinglestoreVersion(any[SinglestoreOptions])).thenReturn("7.1.0")
        when(JdbcHelpers.externalHostPorts(any[SinglestoreOptions]))
          .thenReturn(Map.empty[String, String])

        try {
          spark.read
            .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
            .option("useExternalHost", "true")
            .load(s"$testDb.$testCollection")
            .collect()
          fail("Exception expected")
        } catch {
          case ex: IllegalArgumentException =>
            assert(
              ex.getMessage equals "No external host/port provided for the host 172.17.0.2:3307")
          case _ => fail("IllegalArgumentException expected")
        }
      }
    }

    it("wrong external host") {
      val df = spark.createDF(
        List((2, "B")),
        List(("id", IntegerType, true), ("name", StringType, true))
      )
      writeTable(s"$testDb.$testCollection", df)

      withObjectMocked[JdbcHelpers.type] {

        setupMockJdbcHelper()
        when(JdbcHelpers.getSinglestoreVersion(any[SinglestoreOptions])).thenReturn("7.1.0")

        val externalHostMap = Map(
          "172.17.0.2:3307" -> "somehost:3307"
        )
        when(JdbcHelpers.externalHostPorts(any[SinglestoreOptions]))
          .thenReturn(externalHostMap)

        try {
          spark.read
            .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
            .option("useExternalHost", "true")
            .load(s"$testDb.$testCollection")
            .collect()
          fail("Exception expected")
        } catch {
          case ex: Throwable =>
            ex.getCause match {
              case sqlEx: SQLException =>
                assert(
                  sqlEx.getMessage startsWith "No active connection found for master : Could not connect to HostAddress{host='somehost', port=3307, type='master'}. ")
              case _ => fail("SQLException expected")
            }
        }
      }
    }
  }
}
