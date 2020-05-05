package com.memsql.spark

import java.util.UUID

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.IntegerType

import scala.util.Try

class SQLPermissionsTest extends IntegrationSuiteBase {

  val testUserName   = "sparkuserselect"
  val dbName         = "testdb"
  val collectionName = "temps_test"

  override def beforeEach(): Unit = {
    super.beforeEach()
    val df = spark.createDF(
      List(1, 2, 3),
      List(("id", IntegerType, true))
    )
    writeTable(s"${dbName}.${collectionName}", df)
  }

  private def setUpUserPermissions(privilege: String): Unit = {
    /* Revoke all permissions from user */
    Try(executeQuery(s"REVOKE ALL PRIVILEGES ON ${dbName}.* FROM '${testUserName}'@'%'"))
    /* Give permissions to user */
    executeQuery(s"GRANT ${privilege} ON ${dbName}.* TO '${testUserName}'@'%'")
    /* Set up user to spark */
    spark.conf.set("spark.datasource.memsql.user", s"${testUserName}")
  }

  private def doSuccessOperation(privilege: String)(implicit operation: () => Unit): Unit = {
    it(s"success with ${privilege} permission") {
      setUpUserPermissions(privilege)
      val result = Try(operation())
      if (result.isFailure) {
        result.failed.get.printStackTrace()
        fail()
      }
    }
  }

  private def doFailOperation(privilege: String)(implicit operation: () => Unit): Unit = {
    it(s"fails with ${privilege} permission") {
      setUpUserPermissions(privilege)
      val result = Try(operation())
      /* Error codes description:
        1142 = <command> denied to current user
        1050 = table already exists (error throws when we don't have SELECT permission to check if such table already exists)
       */
      assert(SQLHelper.isSQLExceptionWithCode(result.failed.get, List(1142, 1050)))
    }
  }

  describe("read permissions") {
    /* List of supported privileges for read operation */
    val supportedPrivileges = List("SELECT", "ALL PRIVILEGES")
    /* List of unsupported privileges for read operation */
    val unsupportedPrivileges = List("CREATE", "DROP", "DELETE", "INSERT", "UPDATE")

    implicit def operation(): Unit =
      spark.read.format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT).load(s"${dbName}.${collectionName}")

    unsupportedPrivileges.foreach(doFailOperation)
    supportedPrivileges.foreach(doSuccessOperation)
  }

  describe("write permissions") {
    /* List of supported privileges for write operation */
    val supportedPrivileges = List("INSERT, SELECT", "ALL PRIVILEGES")
    /* List of unsupported privileges for write operation */
    val unsupportedPrivileges = List("CREATE", "DROP", "DELETE", "SELECT", "UPDATE")

    implicit def operation(): Unit = {
      val df = spark.createDF(
        List(4, 5, 6),
        List(("id", IntegerType, true))
      )
      df.write
        .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
        .mode(SaveMode.Append)
        .save(s"${dbName}.${collectionName}")
    }

    unsupportedPrivileges.foreach(doFailOperation)
    supportedPrivileges.foreach(doSuccessOperation)
  }

  describe("drop permissions") {

    /* List of supported privileges for drop operation */
    val supportedPrivileges = List("DROP, SELECT, INSERT", "ALL PRIVILEGES")
    /* List of unsupported privileges for drop operation */
    val unsupportedPrivileges = List("CREATE", "INSERT", "DELETE", "SELECT", "UPDATE")

    implicit def operation(): Unit = {
      val df = spark.createDF(
        List(1, 2, 3),
        List(("id", IntegerType, true))
      )
      df.write
        .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
        .option("truncate", "true")
        .mode(SaveMode.Overwrite)
        .save(s"${dbName}.${collectionName}")
    }

    unsupportedPrivileges.foreach(doFailOperation)
    supportedPrivileges.foreach(doSuccessOperation)
  }

  describe("create permissions") {

    /* List of supported privileges for create operation */
    val supportedPrivileges = List("CREATE, SELECT, INSERT", "ALL PRIVILEGES")
    /* List of unsupported privileges for create operation */
    val unsupportedPrivileges = List("DROP", "INSERT", "DELETE", "SELECT", "UPDATE")

    implicit def operation(): Unit = {
      val df = spark.createDF(
        List(1, 2, 3),
        List(("id", IntegerType, true))
      )
      df.write
        .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
        .mode(SaveMode.Overwrite)
        .save(s"${dbName}.${collectionName}_${UUID.randomUUID().toString.split("-")(0)}")
    }

    unsupportedPrivileges.foreach(doFailOperation)
    supportedPrivileges.foreach(doSuccessOperation)
  }
}
