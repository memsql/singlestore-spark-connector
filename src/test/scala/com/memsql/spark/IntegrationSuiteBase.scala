package com.memsql.spark

import java.sql.{Connection, DriverManager}
import java.util.{Properties, TimeZone}

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest._
import org.scalatest.funspec.AnyFunSpec

import scala.util.Random

trait IntegrationSuiteBase
    extends AnyFunSpec
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with DataFrameComparer
    with LazyLogging {
  final val masterHost: String = sys.props.getOrElse("memsql.host", "localhost")
  final val masterPort: String = sys.props.getOrElse("memsql.port", "5506")

  final val continuousIntegration: Boolean = sys.env
    .getOrElse("CONTINUOUS_INTEGRATION", "false") == "true"

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    // override global JVM timezone to GMT
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"))

    // make memsql use less memory
    executeQuery("set global default_partitions_per_leaf = 2")

    executeQuery("drop database if exists testdb")
    executeQuery("create database testdb")
  }

  override def withFixture(test: NoArgTest): Outcome = {
    def retryThrowable(t: Throwable): Boolean = t match {
      case _: java.sql.SQLNonTransientConnectionException => true
      case _                                              => false
    }

    @scala.annotation.tailrec
    def runWithRetry(attempts: Int, lastError: Option[Throwable]): Outcome = {
      if (attempts == 0) {
        return Canceled(
          s"too many SQLNonTransientConnectionExceptions occurred, last error was:\n${lastError.get}")
      }

      super.withFixture(test) match {
        case Failed(t: Throwable) if retryThrowable(t) || retryThrowable(t.getCause) => {
          Thread.sleep(3000)
          runWithRetry(attempts - 1, Some(t))
        }
        case other => other
      }
    }

    runWithRetry(attempts = 5, None)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    val seed = Random.nextLong()
    log.debug("Random seed: " + seed)
    Random.setSeed(seed)

    if (!continuousIntegration) {
      LogManager.getLogger("com.memsql.spark").setLevel(Level.TRACE)
    }

    spark = SparkSession
      .builder()
      .master("local")
      .appName("memsql-integration-tests")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.driver.bindAddress", "localhost")
      .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
      .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
      .config("spark.sql.session.timeZone", "GMT")
      .config("spark.datasource.memsql.ddlEndpoint", s"${masterHost}:${masterPort}")
      .config("spark.datasource.memsql.user", "root-ssl")
      .config("spark.datasource.memsql.password", "")
      .config("spark.datasource.memsql.enableAsserts", "true")
      .config("spark.datasource.memsql.enableParallelRead", "true")
      .config("spark.datasource.memsql.database", "testdb")
      .config("spark.sql.catalog.spark_catalog", "com.memsql.spark.v2.MemsqlTableCatalog")
      .config("spark.datasource.memsql.useSSL", "true")
      .config("spark.datasource.memsql.serverSslCert",
              s"${System.getProperty("user.dir")}/scripts/ssl/test-ca-cert.pem")
      .config("spark.datasource.memsql.disableSslHostnameVerification", "true")
      .getOrCreate()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    spark.close()
  }

  def jdbcConnection: Loan[Connection] = {
    val connProperties = new Properties()
    connProperties.put("user", "root")

    Loan(
      DriverManager.getConnection(
        s"jdbc:mysql://$masterHost:$masterPort",
        connProperties
      ))
  }

  def executeQuery(sql: String): Unit = {
    log.trace(s"executing query: ${sql}")
    jdbcConnection.to(conn => Loan(conn.createStatement).to(_.execute(sql)))
  }

  def jdbcOptions(dbtable: String): Map[String, String] = Map(
    "url"      -> s"jdbc:mysql://$masterHost:$masterPort",
    "dbtable"  -> dbtable,
    "user"     -> "root",
    "password" -> ""
  )

  def jdbcOptionsSQL(dbtable: String): String =
    jdbcOptions(dbtable)
      .foldLeft(List.empty[String])({
        case (out, (k, v)) => s"'${k}'='${v}'" :: out
      })
      .mkString(", ")

  def writeTable(dbtable: String,
                 df: DataFrame,
                 saveMode: SaveMode = SaveMode.ErrorIfExists): Unit =
    df.write
//      .format("memsql")
      .format("com.memsql.spark.v2")
      .mode(saveMode)
      .save(dbtable)

  def insertValues(dbtable: String,
                   df: DataFrame,
                   onDuplicateKeySQL: String,
                   insertBatchSize: Long): Unit =
    df.write
      .format("memsql")
      .option("onDuplicateKeySQL", onDuplicateKeySQL)
      .option("insertBatchSize", insertBatchSize)
      .mode(SaveMode.Append)
      .save(dbtable)
}
