package com.singlestore.spark

import java.sql.DriverManager
import java.util.{Properties, TimeZone}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest._
import org.scalatest.funspec.AnyFunSpec
import com.singlestore.spark.JdbcHelpers.executeQuery
import com.singlestore.spark.SQLGen.{Relation, SinglestoreVersion}
import com.singlestore.spark.SQLHelper._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{CalendarIntervalType, DecimalType, DoubleType, FloatType, IntegerType, ShortType, StringType}

import scala.util.Random
import scala.util.matching.Regex

trait IntegrationSuiteBase
    extends AnyFunSpec
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with DataFrameComparer
    with LazyLogging {
  object ExcludeFromSpark35 extends Tag("ExcludeFromSpark35")
  object ExcludeFromSpark34 extends Tag("ExcludeFromSpark34")
  object ExcludeFromSpark33 extends Tag("ExcludeFromSpark33")
  object ExcludeFromSpark32 extends Tag("ExcludeFromSpark32")
  object ExcludeFromSpark31 extends Tag("ExcludeFromSpark31")

  final val masterHost: String = sys.props.getOrElse("singlestore.host", "localhost")
  final val masterPort: String = sys.props.getOrElse("singlestore.port", "5506")

  final val continuousIntegration: Boolean = sys.env
    .getOrElse("CONTINUOUS_INTEGRATION", "false") == "true"

  final val masterPassword: String    = sys.env.getOrElse("SINGLESTORE_PASSWORD", "1")
  final val masterJWTPassword: String = sys.env.getOrElse("SINGLESTORE_JWT_PASSWORD", "")
  final val forceReadFromLeaves: Boolean =
    sys.env.getOrElse("FORCE_READ_FROM_LEAVES", "FALSE").equalsIgnoreCase("TRUE")

  var spark: SparkSession = _

  val jdbcDefaultProps = new Properties()
  jdbcDefaultProps.setProperty(JDBCOptions.JDBC_TABLE_NAME, "XXX")
  jdbcDefaultProps.setProperty(JDBCOptions.JDBC_DRIVER_CLASS, "org.mariadb.jdbc.Driver")
  jdbcDefaultProps.setProperty("user", "root")
  jdbcDefaultProps.setProperty("password", masterPassword)

  val version: SinglestoreVersion = {
    val conn =
      DriverManager.getConnection(s"jdbc:mysql://$masterHost:$masterPort", jdbcDefaultProps)
    val resultSet = executeQuery(conn, "select @@memsql_version")
    SinglestoreVersion(resultSet.next().getString(0))
  }

  val canDoParallelReadFromAggregators: Boolean = version.atLeast("7.5.0") && !forceReadFromLeaves

  override def beforeAll(): Unit = {
    // override global JVM timezone to GMT
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"))

    val conn =
      DriverManager.getConnection(s"jdbc:mysql://$masterHost:$masterPort", jdbcDefaultProps)
    try {
      // make singlestore use less memory
      executeQuery(conn, "set global default_partitions_per_leaf = 2")
      executeQuery(conn, "set global data_conversion_compatibility_level = '6.0'")

      executeQuery(conn, "drop database if exists testdb")
      executeQuery(conn, "create database testdb")
    } finally {
      conn.close()
    }
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
      LogManager.getLogger("com.singlestore.spark").setLevel(Level.TRACE)
    }

    spark = SparkSession
      .builder()
      .master(if (canDoParallelReadFromAggregators) "local[2]" else "local")
      .appName("singlestore-integration-tests")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.driver.bindAddress", "localhost")
      .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
      .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
      .config("spark.sql.session.timeZone", "GMT")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.datasource.singlestore.ddlEndpoint", s"${masterHost}:${masterPort}")
      .config("spark.datasource.singlestore.user", "root-ssl")
      .config("spark.datasource.singlestore.password", "")
      .config("spark.datasource.singlestore.aiq_application", "actioniq_hybridcompute")
      .config("spark.datasource.singlestore.enableAsserts", "true")
      .config("spark.datasource.singlestore.enableParallelRead", "automaticLite")
      .config("spark.datasource.singlestore.parallelRead.Features",
              if (forceReadFromLeaves) "ReadFromLeaves" else "ReadFromAggregators,ReadFromLeaves")
      .config("spark.datasource.singlestore.database", "testdb")
      .config("spark.datasource.singlestore.useSSL", "true")
      .config("spark.datasource.singlestore.serverSslCert",
              s"${System.getProperty("user.dir")}/scripts/ssl/test-ca-cert.pem")
      .config("spark.datasource.singlestore.disableSslHostnameVerification", "true")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    spark.close()
  }

  def executeQueryWithLog(sql: String): Unit = {
    log.trace(s"executing query: ${sql}")
    spark.executeSinglestoreQuery(sql)
  }

  def jdbcOptions(dbtable: String): Map[String, String] = Map(
    "url"               -> s"jdbc:mysql://$masterHost:$masterPort",
    "dbtable"           -> dbtable,
    "user"              -> "root",
    "password"          -> masterPassword,
    "pushDownPredicate" -> "false"
  )

  def jdbcOptionsSQL(dbtable: String): String =
    jdbcOptions(dbtable)
      .foldLeft(List.empty[String])({
        case (out, (k, v)) => s"'${k}'='${v}'" :: out
      })
      .mkString(", ")

  def writeTable(dbtable: String, df: DataFrame, saveMode: SaveMode = SaveMode.Overwrite): Unit =
    df.write
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .mode(saveMode)
      .save(dbtable)

  def insertValues(dbtable: String,
                   df: DataFrame,
                   onDuplicateKeySQL: String,
                   insertBatchSize: Long): Unit =
    df.write
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .option("onDuplicateKeySQL", onDuplicateKeySQL)
      .option("insertBatchSize", insertBatchSize)
      .mode(SaveMode.Append)
      .save(dbtable)

  def extractQueriesFromPlan(root: LogicalPlan): Seq[String] = {
    root
      .map({
        case Relation(relation) => relation.sql
        case _                  => ""
      })
      .sorted
  }

  def testCodegenDeterminism(q: String, filterDF: DataFrame => DataFrame): Unit = {
    val logManager    = LogManager.getLogger("com.singlestore.spark.SQLGen$Statement")
    var setLogToTrace = false

    if (logManager.isTraceEnabled) {
      logManager.setLevel(Level.DEBUG)
      setLogToTrace = true
    }

    assert(
      extractQueriesFromPlan(filterDF(spark.sql(q)).queryExecution.optimizedPlan) ==
        extractQueriesFromPlan(filterDF(spark.sql(q)).queryExecution.optimizedPlan),
      "All generated SingleStore queries should be the same"
    )

    if (setLogToTrace) {
      logManager.setLevel(Level.TRACE)
    }
  }

  def testQuery(
    q: String,
    alreadyOrdered: Boolean = false,
    expectPartialPushdown: Boolean = false,
    expectSingleRead: Boolean = false,
    expectEmpty: Boolean = false,
    expectSameResult: Boolean = true,
    expectCodegenDeterminism: Boolean = true,
    pushdown: Boolean = true,
    enableParallelRead: String = "automaticLite",
    dataFrameEqualityPrecision: Double = 0.1,
    filterDF: DataFrame => DataFrame = x => x
  ): Unit = {
    spark.sqlContext.setConf("spark.datasource.singlestore.enableParallelRead", enableParallelRead)

    spark.sql("use testdb_jdbc")
    val jdbcDF = filterDF(spark.sql(q))

    // verify that the jdbc DF works first
    jdbcDF.collect()
    if (pushdown) { spark.sql("use testdb") } else { spark.sql("use testdb_nopushdown") }

    if (expectCodegenDeterminism) {
      testCodegenDeterminism(q, filterDF)
    }

    val singlestoreDF = filterDF(spark.sql(q))

    if (!continuousIntegration) { singlestoreDF.show(4) }

    if (expectEmpty) {
      assert(singlestoreDF.count == 0, "result is expected to be empty")
    } else {
      assert(singlestoreDF.count > 0, "result is expected to not be empty")
    }

    if (expectSingleRead) {
      assert(singlestoreDF.rdd.getNumPartitions == 1,
        "query is expected to read from a single partition")
    } else {
      assert(singlestoreDF.rdd.getNumPartitions > 1,
        "query is expected to read from multiple partitions")
    }

    assert(
      (singlestoreDF.queryExecution.optimizedPlan match {
        case SQLGen.Relation(_) => false
        case _                  => true
      }) == expectPartialPushdown,
      s"the optimized plan does not match expectPartialPushdown=$expectPartialPushdown"
    )

    if (expectSameResult) {
      try {
        def changeTypes(df: DataFrame): DataFrame = {
          var newDf = df
          df.schema
            .foreach(x =>
              x.dataType match {
                // Replace all Floats with Doubles, because JDBC connector converts FLOAT to DoubleType when SingleStore connector converts it to FloatType
                // Replace all Decimals with Doubles, because assertApproximateDataFrameEquality compare Decimals for strong equality
                case _: DecimalType | FloatType =>
                  newDf = newDf.withColumn(x.name, newDf(x.name).cast(DoubleType))
                // Replace all Shorts with Integers, because JDBC connector converts SMALLINT to IntegerType when SingleStore connector converts it to ShortType
                case _: ShortType =>
                  newDf = newDf.withColumn(x.name, newDf(x.name).cast(IntegerType))
                // Replace all CalendarIntervals with Strings, because assertApproximateDataFrameEquality can't sort CalendarIntervals
                case _: CalendarIntervalType =>
                  newDf = newDf.withColumn(x.name, newDf(x.name).cast(StringType))
                case _ =>
              })
          newDf
        }
        assertApproximateDataFrameEquality(changeTypes(singlestoreDF),
          changeTypes(jdbcDF),
          dataFrameEqualityPrecision,
          orderedComparison = alreadyOrdered)
      } catch {
        case e: Throwable =>
          if (continuousIntegration) { println(singlestoreDF.explain(true)) }
          throw e
      }
    }
  }

  def testNoPushdownQuery(q: String, expectSingleRead: Boolean = false): Unit = {
    testQuery(
      q,
      expectPartialPushdown = true,
      pushdown = false,
      expectSingleRead = expectSingleRead
    )
  }

  def testOrderedQuery(q: String, pushdown: Boolean = true): Unit = {
    // order by in SingleStore requires single read
    testQuery(q, alreadyOrdered = true, expectSingleRead = true, pushdown = pushdown)
    afterEach()
    beforeEach()
    testQuery(q,
      alreadyOrdered = true,
      expectPartialPushdown = true,
      expectSingleRead = true,
      pushdown = pushdown,
      enableParallelRead = "automatic")
    afterEach()
    beforeEach()
    testQuery(q,
      alreadyOrdered = true,
      expectSingleRead = true,
      pushdown = pushdown,
      enableParallelRead = "disabled")

  }

  def testSingleReadQuery(
    q: String,
    alreadyOrdered: Boolean = false,
    expectPartialPushdown: Boolean = false,
    expectSameResult: Boolean = true,
    pushdown: Boolean = true
  ): Unit = {
    testQuery(
      q,
      alreadyOrdered = alreadyOrdered,
      expectPartialPushdown = expectPartialPushdown,
      expectSingleRead = true,
      expectSameResult = expectSameResult,
      pushdown = pushdown
    )
  }

  def testSingleReadForReadFromLeaves(q: String): Unit = {
    if (canDoParallelReadFromAggregators) {
      testQuery(q)
    } else {
      testSingleReadQuery(q)
    }
  }

  def testSingleReadForOldS2(
    q: String,
    minVersion: SinglestoreVersion,
    expectPartialPushdown: Boolean = false,
    expectSingleRead: Boolean = false,
    expectSameResult: Boolean = true
  ): Unit = {
    if (version.atLeast(minVersion) && canDoParallelReadFromAggregators) {
      testQuery(
        q,
        expectPartialPushdown = expectPartialPushdown,
        expectSingleRead = expectSingleRead,
        expectSameResult = expectSameResult
      )
    } else {
      testSingleReadQuery(
        q,
        expectPartialPushdown = expectPartialPushdown,
        expectSameResult = expectSameResult
      )
    }
  }

  def bitOperationTest(
    sql: String,
    expectPartialPushdown: Boolean = false,
    expectSingleRead: Boolean = false
  ): Unit = {
    val bitOperationsMinVersion = SinglestoreVersion(7, 0, 1)
    val resultSet               = spark.executeSinglestoreQuery("select @@memsql_version")
    val version                 = SinglestoreVersion(resultSet.next().getString(0))
    if (version.atLeast(bitOperationsMinVersion)) {
      testSingleReadForOldS2(
        sql,
        SinglestoreVersion(7, 6, 0),
        expectPartialPushdown,
        expectSingleRead
      )
    }
  }

  def testUUIDPushdown(q: String): Unit = {
    spark.sql("use testdb_jdbc")
    val jdbcDF = spark.sql(q)

    jdbcDF.collect()
    spark.sql("use testdb")
    val singlestoreDF = spark.sql(q)
    assert(singlestoreDF.schema.equals(jdbcDF.schema))

    val uuidPattern: Regex =
      "[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}".r
    val uuidFieldIndex = singlestoreDF.schema.fieldIndex("uuid()")

    uuidPattern.findFirstMatchIn(singlestoreDF.rdd.first().getString(uuidFieldIndex)) match {
      case Some(_) => None
      case None =>
        throw new IllegalArgumentException(
          "Invalid format of an universally unique identifier (UUID) string generated by Singlestore client"
        )
    }

    uuidPattern.findFirstMatchIn(jdbcDF.rdd.first().getString(uuidFieldIndex)) match {
      case Some(_) => None
      case None =>
        throw new IllegalArgumentException(
          "Invalid format of an universally unique identifier (UUID) string generated by Spark"
        )
    }
  }
}
