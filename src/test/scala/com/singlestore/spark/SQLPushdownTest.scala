package com.singlestore.spark

import com.singlestore.spark.SQLGen.{Relation, SinglestoreVersion}
import com.singlestore.spark.SQLHelper._
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import scala.util.matching.Regex

class SQLPushdownTest extends IntegrationSuiteBase with BeforeAndAfterEach with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    super.beforeEach() // we want to run beforeEach to set up a spark session

    // need to specify explicit schemas - otherwise Spark will infer them
    // incorrectly from the JSON file
    val usersSchema = StructType(
      StructField("id", LongType)
        :: StructField("first_name", StringType)
        :: StructField("last_name", StringType)
        :: StructField("email", StringType)
        :: StructField("owns_house", BooleanType)
        :: StructField("favorite_color", StringType, nullable = true)
        :: StructField("age", IntegerType)
        :: StructField("birthday", DateType)
        :: Nil
    )

    writeTable(
      "testdb.users",
      spark.read.schema(usersSchema).json("src/test/resources/data/users.json")
    )

    val moviesSchema = StructType(
      StructField("id", LongType)
        :: StructField("title", StringType)
        :: StructField("genre", StringType)
        :: StructField("critic_review", StringType, nullable = true)
        :: StructField("critic_rating", FloatType, nullable = true)
        :: Nil
    )

    writeTable(
      "testdb.movies",
      spark.read.schema(moviesSchema).json("src/test/resources/data/movies.json")
    )

    val reviewsSchema = StructType(
      StructField("user_id", LongType)
        :: StructField("movie_id", LongType)
        :: StructField("rating", FloatType)
        :: StructField("review", StringType)
        :: StructField("created", TimestampType)
        :: Nil
    )

    writeTable(
      "testdb.reviews",
      spark.read.schema(reviewsSchema).json("src/test/resources/data/reviews.json")
    )

    writeTable(
      "testdb.users_sample",
      spark.read
        .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
        .load("testdb.users")
        .sample(0.5)
        .limit(10)
    )

    val movieRatingSchema = StructType(
      StructField("id", LongType)
        :: StructField("movie_rating", StringType)
        :: StructField("same_rate_movies", StringType)
        :: Nil
    )

    writeTable(
      "testdb.movies_rating",
      spark.read.schema(movieRatingSchema).json("src/test/resources/data/movies_rating.json")
    )
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    spark.sql("create database testdb")
    spark.sql("create database testdb_nopushdown")
    spark.sql("create database testdb_jdbc")

    def makeTables(sourceTable: String): DataFrame = {
      spark.sql(
        s"""
          |create table testdb.$sourceTable
          |using singlestore options ('dbtable'='testdb.$sourceTable')
          |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
      )
      spark.sql(
        s"""
          |create table testdb_nopushdown.$sourceTable
          |using memsql options ('dbtable'='testdb.$sourceTable','disablePushdown'='true')
          |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
      )
      spark.sql(
        s"""
          |create table testdb_jdbc.$sourceTable
          |using jdbc options (${jdbcOptionsSQL(s"testdb.$sourceTable")})
          |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
      )
    }

    makeTables("users")
    makeTables("users_sample")
    makeTables("movies")
    makeTables("movies_rating")
    makeTables("reviews")

    spark.udf.register("stringIdentity", (s: String) => s)
    spark.udf.register("stringUpper", (s: String) => s.toUpperCase)
    spark.udf.register("integerFilter", (x: Int) => x % 3 == 1)
  }

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
      assert(singlestoreDF.count == 0, "Result is expected to be empty")
    } else {
      assert(singlestoreDF.count > 0, "Result is expected to not be empty")
    }

    if (expectSingleRead) {
      assert(
        singlestoreDF.rdd.getNumPartitions == 1,
        "Query is expected to read from a single partition"
      )
    } else {
      assert(
        singlestoreDF.rdd.getNumPartitions > 1,
        "Query is expected to read from multiple partitions"
      )
    }

    assert(
      (singlestoreDF.queryExecution.optimizedPlan match {
        case SQLGen.Relation(_) => false
        case _                  => true
      }) == expectPartialPushdown,
      s"The Optimized Plan does not match expectPartialPushdown=$expectPartialPushdown"
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
        assertApproximateDataFrameEquality(
          changeTypes(singlestoreDF),
          changeTypes(jdbcDF),
          dataFrameEqualityPrecision,
          orderedComparison = alreadyOrdered
        )
      } catch {
        case e: Throwable =>
          if (continuousIntegration) { println(singlestoreDF.explain(true)) }
          throw e
      }
    }
  }

  def testOrderedQuery(q: String, pushdown: Boolean = true): Unit = {
    // order by in SingleStore requires single read
    testQuery(q, alreadyOrdered = true, expectSingleRead = true, pushdown = pushdown)
    afterEach()
    beforeEach()
    testQuery(
      q,
      alreadyOrdered = true,
      expectPartialPushdown = true,
      expectSingleRead = true,
      pushdown = pushdown,
      enableParallelRead = "automatic"
    )
    afterEach()
    beforeEach()
    testQuery(
      q,
      alreadyOrdered = true,
      expectSingleRead = true,
      pushdown = pushdown,
      enableParallelRead = "disabled"
    )

  }

  def testSingleReadQuery(
    q: String,
    alreadyOrdered: Boolean = false,
    expectPartialPushdown: Boolean = false,
    pushdown: Boolean = true
  ): Unit = {
    testQuery(
      q,
      alreadyOrdered = alreadyOrdered,
      expectPartialPushdown = expectPartialPushdown,
      expectSingleRead = true,
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

  def testSingleReadForOldS2(q: String, minVersion: SinglestoreVersion): Unit = {
    if (version.atLeast(minVersion) && canDoParallelReadFromAggregators) {
      testQuery(q)
    } else {
      testSingleReadQuery(q)
    }
  }

  describe("Attributes") {
    describe("successful pushdown") {
      it("attribute") { testQuery("select id from users") }
      it("alias") { testQuery("select id as user_id from users") }
      it("alias with new line") { testQuery("select id as `user_id\n` from users") }
      it("alias with hyphen") { testQuery("select id as `user-id` from users") }

      // DatasetComparer fails to sort a DataFrame with weird names,
      // because of it following queries are ran as alreadyOrdered

      it("alias with dot") {
        testOrderedQuery("select id as `user.id` from users order by id")
      }
      it("alias with backtick") {
        testOrderedQuery("select id as `user``id` from users order by id")
      }
    }

    describe("unsuccessful pushdown") {
      it("alias with udf") {
        testQuery(
          "select stringIdentity(id) as user_id from users",
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("Literals") {
    describe("successful pushdown") {
      it("string") { testSingleReadForReadFromLeaves("select 'string' from users") }
      it("null") { testSingleReadForReadFromLeaves("select null from users") }

      describe("boolean") {
        it("true") { testQuery("select true from users") }
        it("false") { testQuery("select false from users") }
      }

      it("byte") { testQuery("select 100Y as b_col from users") }
      it("short") { testQuery("select 100S as s_col from users") }
      it("integer") { testQuery("select 100 as i_col from users") }
      it("long") { testSingleReadForReadFromLeaves("select 100L as l_col from users") }
      it("float") { testQuery("select 1.1 as f_col from users") }
      it("double") { testQuery("select 1.1D as d_col from users") }
      it("decimal") { testQuery("select 1.1BD as d_col from users") }
      it("datetime") { testQuery("select date '1997-11-11' as dt_col from users") }
    }

    describe("unsuccessful pushdown") {
      it("interval") {
        testQuery(
          "select interval 1 year 1 month as t_col from users",
          expectPartialPushdown = true
        )
      }
      it("binary literal") {
        testQuery(
          "select X'123456' as t_col from users",
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("Cast") {
    describe("to boolean") {
      it("boolean") { testQuery("select cast(owns_house as boolean) from users") }
      it("int") { testQuery("select cast(age as boolean) from users") }
      it("long") { testQuery("select cast(id as boolean) from users") }
      it("float") { testQuery("select cast(critic_rating as boolean) from movies") }
      it("date") { testQuery("select cast(birthday as boolean) from users") }
      it("timestamp") { testQuery("select cast(created as boolean) from reviews") }
    }

    describe("to byte") {
      it("boolean") { testQuery("select cast(owns_house as byte) from users") }
      it("int") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min TINYINT value spark returns module (452->-60)
        testQuery("select cast(age as byte) from users where age > -129 and age < 128")
      }
      it("long") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min TINYINT value spark returns module (452->-60)
        testQuery("select cast(id as byte) from users where id > -129 and id < 128")
      }
      it("float") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min TINYINT value spark returns module (452->-60)
        // singlestore and spark use different rounding
        // singlestore round to the closest value spark round to the smaller one
        testQuery(
          """
            |select cast(critic_rating as byte)
            |from movies
            |where critic_rating > -129 and critic_rating < 128 and critic_rating - floor(critic_rating) < 0.5
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("date") { testQuery("select cast(birthday as byte) from users") }
      it("timestamp") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min TINYINT value spark returns module (452->-60)
        testQuery(
          """
            |select cast(created as byte)
            |from reviews
            |where cast(created as long) > -129 and cast(created as long) < 128
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
    }

    describe("to short") {
      it("boolean") { testQuery("select cast(owns_house as short) from users") }
      it("int") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min SMALLINT value spark returns module (40004->-25532)
        testQuery("select cast(age as short) from users where age > -32769 and age < 32768")
      }
      it("long") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min SMALLINT value spark returns module (40004->-25532)
        testQuery("select cast(id as short) as d from users where id > -32769 and id < 32768")
      }
      it("float") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min SMALLINT value spark returns module (40004->-25532)
        // singlestore and spark use different rounding
        // singlestore round to the closest value spark round to the smaller one
        testQuery(
          """
            |select cast(critic_rating as short)
            |from movies
            |where critic_rating > -32769 and critic_rating < 32768 and critic_rating - floor(critic_rating) < 0.5
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("date") { testQuery("select cast(birthday as short) from users") }
      it("timestamp") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min SMALLINT value spark returns module (40004->-25532)
        testQuery(
          """
            |select cast(created as short)
            |from reviews
            |where cast(created as long) > -32769 and cast(created as long) < 32768
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
    }

    describe("to int") {
      it("boolean") { testQuery("select cast(owns_house as int) from users") }
      it("int") { testQuery("select cast(age as int) from users") }
      it("long") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min INT value spark returns module (10000000000->1410065408)
        testQuery(
          "select cast(id as int) as d from users where id > -2147483649 and id < 2147483648"
        )
      }
      it("float") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min INT value spark returns module (10000000000->1410065408)
        // singlestore and spark use different rounding
        // singlestore round to the closest value spark round to the smaller one
        testQuery(
          """
            |select cast(critic_rating as int)
            |from movies
            |where critic_rating > -2147483649 and critic_rating < 2147483648 and critic_rating - floor(critic_rating) < 0.5
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("date") { testQuery("select cast(birthday as int) from users") }
      it("timestamp") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min INT value spark returns module (10000000000->1410065408)
        testQuery(
          """
            |select cast(created as int)
            |from reviews
            |where cast(created as long) > -2147483649 and cast(created as long) < 2147483648
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
    }

    describe("to long") {
      it("boolean") { testQuery("select cast(owns_house as long) from users") }
      it("int") { testQuery("select cast(age as long) from users") }
      it("long") { testQuery("select cast(id as long) from users") }
      it("float") {
        // singlestore and spark use different rounding
        // singlestore round to the closest value spark round to the smaller one
        testQuery(
          """
            |select cast(critic_rating as long)
            |from movies
            |where critic_rating - floor(critic_rating) < 0.5
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("date") { testQuery("select cast(birthday as long) from users") }
      it("timestamp") { testQuery("select cast(created as long) from reviews") }
    }

    describe("to float") {
      it("boolean") { testQuery("select cast(owns_house as float) from users") }
      it("int") { testQuery("select cast(age as float) from users") }
      it("long") { testQuery("select cast(id as float) from users") }
      it("float") { testQuery("select cast(critic_rating as float) from movies") }
      it("date") { testQuery("select cast(birthday as float) from users") }
      it("timestamp") { testQuery("select cast(created as float) from reviews") }
    }

    describe("to double") {
      it("boolean") { testQuery("select cast(owns_house as double) from users") }
      it("int") { testQuery("select cast(age as double) from users") }
      it("long") { testQuery("select cast(id as double) from users") }
      it("float") { testQuery("select cast(critic_rating as double) from movies") }
      it("date") { testQuery("select cast(birthday as double) from users") }
      it("timestamp") { testQuery("select cast(created as double) from reviews") }
    }

    describe("to decimal") {
      it("boolean") { testQuery("select cast(owns_house as decimal(20, 5)) from users") }
      it("int") { testQuery("select cast(age as decimal(20, 5)) from users") }
      it("long") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min DECIMAL(x, y) value spark returns null
        testQuery("select cast(id as decimal(20, 5)) from users where id < 999999999999999.99999")
      }
      it("float") { testQuery("select cast(critic_rating as decimal(20, 5)) from movies") }
      it("date") { testQuery("select cast(birthday as decimal(20, 5)) from users") }
      it("timestamp") { testQuery("select cast(created as decimal(20, 5)) from reviews") }
    }

    describe("to string") {
      it("boolean") { testQuery("select cast(owns_house as string) from users") }
      it("int") { testQuery("select cast(age as string) from users") }
      it("long") { testQuery("select cast(id as string) from users") }
      it("float") {
        // singlestore converts integers to string with 0 digits
        // after the point spark adds 1 digit after the point
        testQuery("select cast(cast(critic_rating as string) as float) from movies")
      }
      it("date") { testQuery("select cast(birthday as string) from users") }
      it("timestamp") {
        // singlestore converts timestamps to string with 6 digits
        // after the point spark adds 0 digit after the point
        testQuery("select cast(cast(created as string) as timestamp) from reviews")
      }
      it("string") { testQuery("select cast(first_name as string) from users") }
    }

    describe("to binary") {
      // spark doesn't support casting other types to binary
      it("string") { testQuery("select cast(first_name as binary) from users") }
    }

    describe("to date") {
      it("date") { testQuery("select cast(birthday as date) from users") }
      it("timestamp") { testQuery("select cast(created as date) from reviews") }
      it("string") { testQuery("select cast(cast(birthday as string) as date) from users") }
    }

    describe("to timestamp") {
      it("boolean") { testQuery("select cast(owns_house as timestamp) from users") }
      it("int") { testQuery("select cast(age as timestamp) from users") }
      it("long") {
        // TIMESTAMP in SingleStore doesn't support values greater then 2147483647999
        testQuery("select cast(id as timestamp) from users where id < 2147483647999")
      }
      it("float") {
        // singlestore and spark use different rounding
        // singlestore round to the closest value spark round to the smaller one
        testQuery(
          """
            |select to_unix_timestamp(cast(critic_rating as timestamp)) as t_col
            |from movies
            |where critic_rating - floor(critic_rating) < 0.5
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("date") { testQuery("select cast(birthday as timestamp) from users") }
      it("timestamp") { testQuery("select cast(created as timestamp) from reviews") }
      it("string") { testQuery("select cast(cast(birthday as string) as timestamp) from users") }
    }
  }

  describe("Variable Expressions") {
    describe("Coalesce") {
      it("one non-null value") { testQuery("select coalesce(id) as t_col from users") }
      it("one null value") {
        testSingleReadForReadFromLeaves("select coalesce(null) as t_col from users")
      }
      it("a lot of values") {
        testQuery("select coalesce(null, id, null, id + 1) as t_col from users")
      }
      it("a lot of nulls") {
        testSingleReadForReadFromLeaves("select coalesce(null, null, null) as t_col from users")
      }
      it("partial pushdown with udf") {
        testQuery(
          """
            |select coalesce(stringIdentity(first_name), 'qwerty', 'bob', 'alice') as t_col
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    describe("Least") {
      it("a lot of ints") { testQuery("select least(id+5, id, 5, id+1) as t_col from users") }
      it("a lot of strings") {
        testQuery("select least('qwerty', 'bob', first_name, 'alice') as t_col from users")
      }
      // singlestore returns NULL if at least one argument is NULL, when spark skips nulls
      ignore("09/2024 - ints with null") {
        testQuery("select least(null, id, null, id+1) as t_col from users")
      }
      it("a lot of nulls") {
        testSingleReadForReadFromLeaves("select least(null, null, null) as t_col from users")
      }
      it("partial pushdown with udf") {
        testQuery(
          "select least('qwerty', 'bob', stringIdentity(first_name), 'alice') as t_col from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("Greatest") {
      it("a lot of ints") { testQuery("select greatest(id+5, id, 5, id+1) as t_col from users") }
      it("a lot of strings") {
        testQuery("select greatest('qwerty', 'bob', first_name, 'alice') as t_col from users")
      }
      // singlestore returns NULL if at least one argument is NULL, when spark skips nulls
      ignore("09/2024 - ints with null") {
        testQuery("select greatest(null, id, null, id+1) as t_col from users")
      }
      it("a lot of nulls") {
        testSingleReadForReadFromLeaves("select greatest(null, null, null) as t_col from users")
      }
      it("partial pushdown with udf") {
        testQuery(
          """
            |select greatest('qwerty', 'bob', stringIdentity(first_name), 'alice') as t_col
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    describe("Concat") {
      it("a lot of ints") { testQuery("select concat(id+5, id, 5, id+1) as t_col from users") }
      it("a lot of strings") {
        testQuery("select concat('qwerty', 'bob', first_name, 'alice') as t_col from users")
      }
      it("ints with null") { testQuery("select concat(null, id, null, id+1) as t_col from users") }
      it("a lot of nulls") {
        testSingleReadForReadFromLeaves("select concat(null, null, null) as t_col from users")
      }
      it("int and string") { testQuery("select concat(id, first_name) as t_col from users") }
      it("same expressions") { testQuery("select concat(id, id, id) as t_col from users") }
      it("nested same expressions") {
        testQuery("select * from (select concat(id, id, id) as t_col from users)")
      }
      it("partial pushdown with udf") {
        testQuery(
          "select concat('qwerty', 'bob', stringIdentity(first_name), 'alice') as t_col from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("Elt") {
      it("a lot of ints") { testQuery("select elt(id+5, id, 5, id+1) as t_col from users") }
      it("a lot of strings") {
        testQuery("select elt('qwerty', 'bob', first_name, 'alice') as t_col from users")
      }
      it("ints with null") { testQuery("select elt(null, id, null, id+1) as t_col from users") }
      it("a lot of nulls") { testQuery("select elt(null, null, null) as t_col from users") }
      it("int and string") { testQuery("select elt(id, first_name) as t_col from users") }
      it("partial pushdown with udf") {
        testQuery(
          "select elt('qwerty', 'bob', stringIdentity(first_name), 'alice') as t_col from users",
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("Null Expressions") {
    describe("IfNull") {
      it("returns the second argument") {
        testQuery("select IfNull(null, id) as ifn from users")
      }
      it("returns the first argument") {
        testQuery("select IfNull(id, null) as ifn from users")
      }
      it("partial pushdown with udf") {
        testQuery(
          "select IfNull(stringIdentity(id), id) as ifn from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("NullIf") {
      it("equal arguments") {
        testQuery("select id, NullIf(1, 1) as nif from users")
      }
      it("non-equal arguments") {
        testQuery("select NullIf(id, null) as nif from users")
      }
      it("partial pushdown with udf") {
        testQuery(
          "select NullIf(null, stringIdentity(id)) as nif from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("Nvl") {
      it("returns the second argument") {
        testQuery("select Nvl(null, id) as nvl from users")
      }
      it("returns the first argument") {
        testQuery("select Nvl(id, id+1) as nvl from users")
      }
      it("partial pushdown with udf") {
        testQuery(
          "select Nvl(stringIdentity(id), null) as nvl from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("Nvl2") {
      it("returns the second argument") {
        testSingleReadForReadFromLeaves("select Nvl2(null, id, 0) as nvl2 from users")
      }
      it("returns the first argument") {
        testQuery("select Nvl2(id, 10, id+1) as nvl2 from users")
      }
      it("partial pushdown with udf") { // error
        testQuery(
          "select Nvl2(stringIdentity(id), null, id) as nvl2 from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("IsNull") {
      it("not null") {
        testQuery("select IsNull(favorite_color) as isn from users")
      }
      it("null") { testQuery("select IsNull(null) as isn from users") }
      it("partial pushdown with udf") {
        testQuery(
          "select IsNull(stringIdentity(id)) as isn from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("IsNotNull") {
      it("not null") {
        testQuery("select IsNotNull(favorite_color) as isnn from users")
      }
      it("null") { testQuery("select IsNotNull(null) as isnn from users") }
      it("partial pushdown with udf") {
        testQuery(
          "select IsNotNull(stringIdentity(id)) as isnn from users",
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("Predicates") {
    describe("Not") {
      it("not null") {
        testQuery("select not(cast(owns_house as boolean)) from users")
      }
      it("null") { testQuery("select not(null) from users") }
      it("partial pushdown with udf") {
        testQuery(
          "select not(cast(stringIdentity(id) as boolean)) from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("If") {
      it("boolean") {
        testQuery(
          "select if(cast(owns_house as boolean), first_name, last_name) as t_col from users"
        )
      }
      it("always true") { testQuery("select if(true, first_name, last_name) as t_col from users") }
      it("partial pushdown with udf") {
        testQuery(
          """
            |select if(cast(stringIdentity(id) as boolean), first_name, last_name) as t_col
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    describe("case when") {
      it("simple") {
        testQuery("select case when id < 10 then 1 else 3 end from users_sample")
      }
      it("match multiple conditions") {
        testQuery(
          """
            |select
            |case id
            | when id < 3 then id
            | when id < 6 and id >= 3 then age
            | when id < 9 and id >=6 then last_name
            | else first_name
            |end as t_col
            |from users_sample
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("match else condition") {
        testQuery(
          """
            |select
            | id,
            | case
            |   when 1 < 0 then id
            |   when 2 < 0 or 3 < 0 then last_name
            |   else first_name
            | end as t_col
            |from users_sample
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("without else condition, select NULL") {
        testQuery("select id, case when 1 < 0 then id end from users_sample")
      }
      it("complicated case: comparing string with a numeric type") {
        testQuery(
          """
            |select
            | id,
            | case last_name
            |   when id < '5' then 'No_name'
            |   when 'Paute' then 'Random_surname'
            |   else last_name
            | end as t_col
            |from users_sample
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          // when string is casted to numeric type, singlestore takes the prefix of it
          // which is numeric (spark returns null if the whole string is not numeric)
          expectSameResult = false
        )
      }
      it("partial pushdown: invalid condition") {
        testQuery(
          "select case when IsNull(stringIdentity(id)) then 1 else id end as t_col from users",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown: invalid condition, more complicated query") {
        testQuery(
          """
            |select
            | case
            |   when id < 2 then last_name
            |   when IsNull(stringIdentity(id)) then 1
            |   else id
            | end as t_col
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
      it("partial pushdown: invalid `else` condition") {
        testQuery(
          "select case when id < 5 then 1 else stringIdentity(id) end as t_col from users",
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("arithmetic") {
    describe("Add") {
      it("numbers") { testQuery("select user_id + movie_id as t_col from reviews") }
      it("floats") { testQuery("select rating + 1.0 as t_col from reviews") }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          "select stringIdentity(user_id) + movie_id as t_col from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          "select user_id + stringIdentity(movie_id) as t_col from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("Subtract") {
      it("numbers") { testQuery("select user_id - movie_id as t_col from reviews") }
      it("floats") { testQuery("select rating - 1.0 as t_col from reviews") }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          "select stringIdentity(user_id) - movie_id as t_col from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          "select user_id - stringIdentity(movie_id) as t_col from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("Multiply") {
      it("numbers") { testQuery("select user_id * movie_id as t_col from reviews") }
      it("floats") { testQuery("select rating * 1.3 as t_col from reviews") }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          "select stringIdentity(user_id) * movie_id as t_col from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          "select user_id * stringIdentity(movie_id) as t_col from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("Divide") {
      it("numbers") { testQuery("select user_id / movie_id as t_col from reviews") }
      it("floats") { testQuery("select rating / 1.3 as t_col from reviews") }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          "select stringIdentity(user_id) / movie_id as t_col from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          "select user_id / stringIdentity(movie_id) as t_col from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("Remainder") {
      it("numbers") { testQuery("select user_id % movie_id as t_col from reviews") }
      it("floats") { testQuery("select rating % 4 as t_col from reviews") }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          "select stringIdentity(user_id) % movie_id as t_col from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          "select user_id % stringIdentity(movie_id) as t_col from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("Pmod") {
      it("numbers") { testQuery("select pmod(user_id, movie_id) as t_col from reviews") }
      it("floats") { testQuery("select pmod(rating, 4) as t_col from reviews") }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          "select pmod(stringIdentity(user_id), movie_id) as t_col from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          "select pmod(user_id, stringIdentity(movie_id)) as t_col from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("UnaryMinus") {
      it("numbers") { testQuery("select -id from users") }
      it("floats") { testQuery("select -critic_rating from movies") }
      it("partial pushdown with udf") {
        testQuery("select -stringIdentity(id) from users", expectPartialPushdown = true)
      }
    }

    describe("UnaryPositive") {
      it("numbers") { testQuery("select +id from users") }
      it("floats") { testQuery("select +critic_rating from movies") }
      it("partial pushdown with udf") {
        testQuery("select +stringIdentity(id) from users", expectPartialPushdown = true)
      }
    }

    describe("Abs") {
      it("positive numbers") { testQuery("select abs(id) from users") }
      it("negative numbers") { testQuery("select abs(-id) from users") }
      it("positive floats") { testQuery("select abs(critic_rating) from movies") }
      it("negative floats") { testQuery("select abs(-critic_rating) from movies") }
      it("partial pushdown with udf") {
        testQuery("select abs(stringIdentity(id)) from users", expectPartialPushdown = true)
      }
    }
  }

  describe("bitwiseExpressions") {
    describe("And") {
      it("succeeds") { testQuery("select user_id & movie_id as t_col from reviews") }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          "select cast(stringIdentity(user_id) as integer) & movie_id as t_col from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          "select user_id & cast(stringIdentity(movie_id) as integer) as t_col from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("Or") {
      it("numbers") { testQuery("select user_id | movie_id as t_col from reviews") }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          "select cast(stringIdentity(user_id) as integer) | movie_id as t_col from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          "select user_id | cast(stringIdentity(movie_id) as integer) as t_col from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("Xor") {
      it("numbers") { testQuery("select user_id ^ movie_id as t_col from reviews") }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          "select cast(stringIdentity(user_id) as integer) ^ movie_id as t_col from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          "select user_id ^ cast(stringIdentity(movie_id) as integer) as t_col from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("bitwiseGet") {
      it("numbers", ExcludeFromSpark31) {
        testQuery("select bit_get(id, 2) from users_sample")
      }
      it("negative left argument", ExcludeFromSpark31) {
        try {
          testQuery("select bit_get(id, -2) from users_sample")
        } catch {
          case e: Throwable =>
            if (e.toString.contains("Invalid bit position: -2 is less than zero")) {
              None
            } else { throw e }
        }
      }
      it("negative right argument", ExcludeFromSpark31) {
        testQuery("select bit_get(-200, 2), id from users_sample")
      }
      it("exceeds upper limit left argument", ExcludeFromSpark31) {
        try {
          testQuery("select bit_get(id, 100000) from users_sample")
        } catch {
          case e: Throwable =>
            if (e.toString.contains("Invalid bit position: 100000 exceeds the bit upper limit")) {
              None
            } else { throw e }
        }
      }
      it("big int right argument", ExcludeFromSpark31) {
        testQuery("select bit_get(1000000000000000, 2), id from users_sample")
      }
      it("partial pushdown because of udf in the left argument", ExcludeFromSpark31) {
        testQuery(
          "select bit_get(cast(stringIdentity(movie_id) as integer), 2) from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument", ExcludeFromSpark31) {
        testQuery(
          "select bit_get(movie_id, cast(stringIdentity(2) as integer)) from reviews",
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("sanity test disablePushdown") {
    def testNoPushdownQuery(q: String, expectSingleRead: Boolean = false): Unit =
      testQuery(q,
                expectPartialPushdown = true,
                pushdown = false,
                expectSingleRead = expectSingleRead)

    it("select all users") { testNoPushdownQuery("select * from users") }
    it("select all movies") { testNoPushdownQuery("select * from movies") }
    it("select all reviews") { testNoPushdownQuery("select * from reviews") }
    it("basic filter") { testNoPushdownQuery("select * from users where id = 1") }
    it("basic agg") {
      testNoPushdownQuery("select floor(avg(age)) from users", expectSingleRead = true)
    }
    it("numeric order") {
      testNoPushdownQuery("select * from users order by id asc", expectSingleRead = true)
    }
    it("limit with sort") {
      testNoPushdownQuery("select * from users order by id limit 10", expectSingleRead = true)
    }
    it("implicit inner join") {
      testNoPushdownQuery(
        "select * from users as a, reviews as b where a.id = b.user_id",
        expectSingleRead = true
      )
    }
  }

  describe("sanity test the tables") {
    it("select all users") { testQuery("select * from users") }
    it("select all users (sampled)") { testQuery("select * from users_sample") }
    it("select all movies") { testQuery("select * from movies") }
    it("select all reviews") { testQuery("select * from reviews") }
  }

  describe("math expressions") {
    describe("sinh") {
      it("works with float") { testQuery("select sinh(rating) as sinh from reviews") }
      it("works with tinyint") { testQuery("select sinh(owns_house) as sinh from users") }
      it("partial pushdown") {
        testQuery(
          "select sinh(stringIdentity(rating)) as sinh, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select sinh(null) as sinh from reviews") }
    }

    describe("cosh") {
      it("works with float") { testQuery("select cosh(rating) as cosh from reviews") }
      it("works with tinyint") { testQuery("select cosh(owns_house) as cosh from users") }
      it("partial pushdown") {
        testQuery(
          "select cosh(stringIdentity(rating)) as cosh, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select cosh(null) as cosh from reviews") }
    }

    describe("tanh") {
      it("works with float") { testQuery("select tanh(rating) as tanh from reviews") }
      it("works with tinyint") { testQuery("select tanh(owns_house) as tanh from users") }
      it("partial pushdown") {
        testQuery(
          "select tanh(stringIdentity(rating)) as tanh, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select tanh(null) as tanh from reviews") }
    }

    describe("hypot") {
      it("works with float") { testQuery("select hypot(rating, user_id) as hypot from reviews") }
      it("works with tinyint") { testQuery("select hypot(owns_house, id) as hypot from users") }
      it("partial pushdown") {
        testQuery(
          "select hypot(stringIdentity(rating), user_id) as hypot, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select hypot(null, null) as hypot from reviews") }
    }

    describe("rint") {
      it("works with float") { testQuery("select rint(rating) as rint from reviews") }
      it("works with tinyint") { testQuery("select rint(owns_house) as rint from users") }
      it("partial pushdown") {
        testQuery(
          "select rint(stringIdentity(rating)) as rint, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select rint(null) as rint from reviews") }
    }

    describe("asinh") {
      it("works with float") { testQuery("select rating, asinh(rating) as t_col from reviews") }
      it("works with tinyint") { testQuery("select owns_house, asinh(owns_house) as t_col from users") }
      it("partial pushdown") {
        testQuery(
          "select rating, asinh(stringIdentity(rating)) as t_col from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select asinh(null) as t_col from reviews") }
    }

    describe("acosh") {
      it("works with float") { testQuery("select acosh(rating) as acosh from reviews") }
      it("partial pushdown") {
        testQuery(
          "select acosh(stringIdentity(rating)) as acosh, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select acosh(null) as acosh from reviews") }
    }

    describe("atanh") {
      it("works with float") {
        testQuery(
          """
            |select
            | critic_rating,
            | atanh(critic_rating) as t_col
            |from movies
            |where critic_rating > -1 AND critic_rating < 1
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("partial pushdown") {
        testQuery(
          "select rating, atanh(stringIdentity(rating)) as t_col from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select atanh(null) as t_col from reviews") }
    }

    describe("integralDivide") {
      it("works with bigint") {
        testQuery("select user_id, movie_id, user_id div movie_id from reviews")
      }
      it("works with null") { testQuery("select null div null as integralDivide from reviews") }
      it("partial pushdown") {
        testQuery(
          "select stringIdentity(null div null) as integralDivide from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("sqrt") {
      it("works with float") { testQuery("select sqrt(rating) as sqrt from reviews") }
      it("works with tinyint") { testQuery("select sqrt(owns_house) as sqrt from users") }
      it("partial pushdown") {
        testQuery(
          "select sqrt(stringIdentity(rating)) as sqrt, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select sqrt(null) as sqrt from reviews") }
    }

    describe("ceil") {
      it("works with float") { testQuery("select ceil(rating) as ceil from reviews") }
      it("works with tinyint") { testQuery("select ceil(owns_house) as ceil from users") }
      it("partial pushdown") {
        testQuery(
          "select ceil(stringIdentity(rating)) as ceil, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select ceil(null) as ceil from reviews") }
    }

    describe("cos") {
      it("works with float") { testQuery("select cos(rating) as cos from reviews") }
      it("works with tinyint") { testQuery("select cos(owns_house) as cos from users") }
      it("partial pushdown") {
        testQuery(
          "select cos(stringIdentity(rating)) as cos, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select cos(null) as cos from reviews") }
    }

    describe("exp") {
      it("works with float") { testQuery("select exp(rating) as exp from reviews") }
      it("works with tinyint") { testQuery("select exp(owns_house) as exp from users") }
      it("partial pushdown") {
        testQuery(
          "select exp(stringIdentity(rating)) as exp, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select exp(null) as exp from reviews") }
    }

    describe("expm1") {
      it("works with float") { testQuery("select expm1(rating) as expm1 from reviews") }
      it("works with tinyint") { testQuery("select expm1(owns_house) as expm1 from users") }
      it("partial pushdown") {
        testQuery(
          "select expm1(stringIdentity(rating)) as expm1, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select expm1(null) as expm1 from reviews") }
    }

    describe("floor") {
      it("works with float") { testQuery("select floor(rating) as floor from reviews") }
      it("works with tinyint") { testQuery("select floor(owns_house) as floor from users") }
      it("partial pushdown") {
        testQuery(
          "select floor(stringIdentity(rating)) as floor, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select floor(null) as floor from reviews") }
    }

    describe("log") {
      it("works with float") { testQuery("select log(rating) as log from reviews") }
      it("works with tinyint") { testQuery("select log(owns_house) as log from users") }
      it("partial pushdown") {
        testQuery(
          "select log(stringIdentity(rating)) as log, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select log(null) as log from reviews") }
    }

    describe("log2") {
      it("works with float") { testQuery("select log2(rating) as log2 from reviews") }
      it("works with tinyint") { testQuery("select log2(owns_house) as log2 from users") }
      it("partial pushdown") {
        testQuery(
          "select log2(stringIdentity(rating)) as log2, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select log2(null) as log2 from reviews") }
    }

    describe("log10") {
      it("works with float") { testQuery("select log10(rating) as log10 from reviews") }
      it("works with tinyint") { testQuery("select log10(owns_house) as log10 from users") }
      it("partial pushdown") {
        testQuery(
          "select log10(stringIdentity(rating)) as log10, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select log10(null) as log10 from reviews") }
    }

    describe("log1p") {
      it("works with float") { testQuery("select log1p(rating) as log1p from reviews") }
      it("works with tinyint") { testQuery("select log1p(owns_house) as log1p from users") }
      it("partial pushdown") {
        testQuery(
          "select log1p(stringIdentity(rating)) as log1p, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select log1p(null) as log1p from reviews") }
    }

    describe("signum") {
      it("works with float") { testQuery("select signum(rating) as signum from reviews") }
      it("works with tinyint") { testQuery("select signum(owns_house) as signum from users") }
      it("partial pushdown") {
        testQuery(
          "select signum(stringIdentity(rating)) as signum, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select signum(null) as signum from reviews") }
    }

    describe("cot") {
      it("works with float") { testQuery("select cot(rating) as cot from reviews") }
      it("partial pushdown") {
        testQuery(
          "select cot(stringIdentity(rating)) as cot, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select cot(null) as cot from reviews") }
    }

    describe("toDegrees") {
      it("works with float") { testQuery("select degrees(rating) as degrees from reviews") }
      it("works with tinyint") { testQuery("select degrees(owns_house) as degrees from users") }
      it("partial pushdown") {
        testQuery(
          "select degrees(stringIdentity(rating)) as degrees, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select degrees(null) as degrees from reviews") }
    }

    describe("toRadians") {
      it("works with float") { testQuery("select radians(rating) as radians from reviews") }
      it("works with tinyint") { testQuery("select radians(owns_house) as radians from users") }
      it("partial pushdown") {
        testQuery(
          "select radians(stringIdentity(rating)) as radians, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select radians(null) as radians from reviews") }
    }

    describe("bin") {
      it("works with float") { testQuery("select bin(user_id) as bin from reviews") }
      it("works with tinyint") { testQuery("select bin(owns_house) as bin from users") }
      it("partial pushdown") {
        testQuery(
          "select bin(stringIdentity(rating)) as bin, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testSingleReadForReadFromLeaves("select bin(null) as bin from reviews") }
    }

    describe("hex") {
      it("works with float") { testQuery("select hex(user_id) as hex from reviews") }
      it("works with tinyint") { testQuery("select hex(owns_house) as hex from users") }
      it("partial pushdown") {
        testQuery(
          "select hex(stringIdentity(rating)) as hex, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testSingleReadForReadFromLeaves("select hex(null) as hex from reviews") }
    }

    describe("unhex") {
      it("works with tinyint") { testQuery("select unhex(owns_house) as unhex from users") }
      it("partial pushdown") {
        testQuery(
          "select unhex(stringIdentity(rating)) as unhex, stringIdentity(review) as review from reviews",
          expectPartialPushdown = true
        )
      }
      it("works with null") { testQuery("select unhex(null) as unhex from reviews") }
    }

    it("sinh") { testQuery("select sinh(rating) as sinh from reviews") }
    it("cosh") { testQuery("select cosh(rating) as cosh from reviews") }
    it("tanh") { testQuery("select tanh(rating) as tanh from reviews") }
    it("hypot") { testQuery("select hypot(rating, user_id) as hypot from reviews") }
    it("rint") { testQuery("select rint(rating) as rint from reviews") }

    describe("Atan2") {
      // atan(-e,-1) = -pi
      // atan(e,-1) = pi
      // where e is a very small value
      // we are filtering this cases, because the result can differ
      // for singlestore and spark, because of precision loss
      it("works") {
        testQuery(
        """
          |select
          | atan2(critic_rating, id) as a1,
          | atan2(critic_rating, -id) as a2,
          | atan2(-critic_rating, id) as a3,
          | atan2(-critic_rating, -id) as a4,
          | critic_rating, id
          |from movies
          |where abs(critic_rating) > 0.01 or critic_rating is null
          |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("udf in the left argument") {
        testQuery(
          "select atan2(stringIdentity(critic_rating), id) as t_col from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the right argument") {
        testQuery(
          "select atan2(critic_rating, stringIdentity(id)) as t_col from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("Pow") {
      it("works") { testQuery("select log(pow(id, critic_rating)) as t_col from movies") }
      it("udf in the left argument") {
        testQuery(
          "select log(pow(stringIdentity(id), critic_rating)) as t_col from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the right argument") {
        testQuery(
          "select log(pow(id, stringIdentity(critic_rating))) as t_col from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("Logarithm") {
      it("works") {
        // spark returns +/-Infinity for log(1, x) singlestore returns NULL for it
        testQuery(
        """
          |select
          | log(critic_rating, id) as l1,
          | log(critic_rating, -id) as l2,
          | log(-critic_rating, id) as l3,
          | log(-critic_rating, -id) as l4,
          | log(id, critic_rating) as l5,
          | log(id, -critic_rating) as l6,
          | log(-id, critic_rating) as l7,
          | log(-id, -critic_rating) as l8,
          | critic_rating,
          | id
          |from movies
          |where id != 1 and critic_rating != 1
          |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("works with 1 argument") { testQuery("select log(critic_rating) as t_col from movies") }
      it("udf in the left argument") {
        testQuery(
          "select log(stringIdentity(id), critic_rating) as t_col from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the right argument") {
        testQuery(
          "select log(id, stringIdentity(critic_rating)) as t_col from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("Round") {
      // singlestore can round x.5 differently
      it("works with one argument") {
        testQuery(
        """
          |select
          | round(critic_rating),
          | round(-critic_rating),
          | critic_rating
          |from movies
          |where critic_rating - floor(critic_rating) != 0.5
          |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("works with two arguments") {
        testQuery(
        """
          |select
          | round(critic_rating/10.0, 1) as x1,
          | round(-critic_rating/100.0, 2) as x2,
          | critic_rating
          |from movies
          |where critic_rating - floor(critic_rating) != 0.5
          |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("works with negative scale") { testQuery("select round(critic_rating, -2) as t_col from movies") }
      it("udf in the left argument") {
        testQuery(
          "select round(stringIdentity(critic_rating), 2) as t_col from movies",
          expectPartialPushdown = true
        )
      }
      // right argument must be foldable
      // because of it, we can't use udf there
    }

    describe("WidthBucket") {
      it("works with int, simple case") {
        testQuery("select id, width_bucket(id, 10, 90, 10) as t_col from movies")
      }
      it("works with int, negative value") {
        testQuery("select width_bucket(-id, -90, -10, 10) as t_col from movies")
      }
      it("many small buckets") {
        testQuery("select id, width_bucket(id, 10, 90, 1000000000) as t_col from movies")
      }
      it("works with int, max < min") {
        testQuery("select id, width_bucket(id, 90, 10, 10) as t_col from movies")
      }
      it("num_buckets is float") {
        testQuery("select id, width_bucket(id, 90, 10, 6.6) as t_col from movies")
      }
      it("num_buckets is non-constant  float") {
        testQuery(
          "select id, width_bucket(id, 10, 90, id/10 + 1) as t_col from movies where id/10 - floor(id/10) < 0.5"
        )
      }
      it("works with float, simple case") {
        testQuery(
          "select id, width_bucket(critic_rating, 10 + critic_rating, 90, 10) as t_col from movies")
      }
      it("works with string") {
        testQuery("select id, width_bucket(-critic_rating, '10.2', '0.4', '10') as t_col from movies")
      }
      it("all args are not const") {
        testQuery("select id, width_bucket(critic_rating, id-10, id, id + 200) as t_col from movies")
      }
      it("works with int, max = min") {
        testQuery("select id, width_bucket(id, 90, 90, 10) as t_col from movies")
      }
      it("num_buckets is negative") {
        testQuery("select id, width_bucket(id, 90, 10, -1) as t_col from movies")
      }
      it("num_buckets = 0") {
        testQuery("select id, width_bucket(id, 90, 10, 0) as t_col from movies")
      }
      it("udf in the first arg") {
        testQuery(
          "select id, width_bucket(stringIdentity(critic_rating), 10, 100, 200) as t_col from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the second arg") {
        testQuery(
          "select id, width_bucket(id, stringIdentity(critic_rating), 100, 200) as t_col from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the third arg") {
        testQuery(
          "select id, width_bucket(id, 10, stringIdentity(critic_rating), 200) as t_col from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the fourth arg") {
        testQuery(
          "select id, width_bucket(id, 100, 200, stringIdentity(critic_rating)) as t_col from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("Hypot") {
      it("works") {
        testQuery(
        """
          |select
          | Hypot(critic_rating, id) as h1,
          | Hypot(critic_rating, -id) as h2,
          | Hypot(-critic_rating, id) as h3,
          | Hypot(-critic_rating, -id) as h4,
          | Hypot(id, critic_rating) as h5,
          | Hypot(id, -critic_rating) as h6,
          | Hypot(-id, critic_rating) as h7,
          | Hypot(-id, -critic_rating) as h8,
          | critic_rating,
          | id
          |from movies
          |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("udf in the left argument") {
        testQuery(
          "select Hypot(stringIdentity(critic_rating), id) as t_col from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the right argument") {
        testQuery(
          "select Hypot(critic_rating, stringIdentity(id)) as t_col from movies",
          expectPartialPushdown = true
        )
      }
    }

    it("EulerNumber") { testQuery("select E() from movies") }
    it("Pi") { testQuery("select PI() from movies") }

    describe("Conv") {
      // singlestore and spark behaviour differs when num contains non alphanumeric characters
      val bases = Seq(2, 5, 23, 36)
      it("works") {
        for (fromBase <- bases; toBase <- bases) {
          log.debug(s"testing conv $fromBase -> $toBase")
          testQuery(
            s"""
              |select
              |  first_name,
              |  conv(first_name, $fromBase, $toBase)
              |from users
              |where first_name rlike '^[a-zA-Z0-9]*$$'
              |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
          )
        }
      }
      it("works with numeric") { testQuery("select conv(id, 10, 2) from users") }
      it("partial pushdown when fromBase out of range [2, 36]") {
        testQuery("select conv(first_name, 1, 20) from users", expectPartialPushdown = true)
      }
      it("partial pushdown when toBase out of range [2, 36]") {
        testQuery("select conv(first_name, 20, 1) from users", expectPartialPushdown = true)
      }
      it("udf in the first argument") {
        testQuery(
          "select conv(stringIdentity(first_name), 20, 15) from users",
          expectPartialPushdown = true
        )
      }
      it("udf in the second argument") {
        testQuery(
          "select conv(first_name, stringIdentity(20), 15) from users",
          expectPartialPushdown = true
        )
      }
      it("udf in the third argument") {
        testQuery(
          "select conv(first_name, 20, stringIdentity(15)) from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("ShiftLeft") {
      it("works") {
        testQuery("select ShiftLeft(id, floor(critic_rating)) as t_col from movies")
      }
      it("udf in the left argument") {
        testQuery(
          "select ShiftLeft(stringIdentity(id), floor(critic_rating)) as t_col from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the right argument") {
        testQuery(
          "select ShiftLeft(id, stringIdentity(floor(critic_rating))) as t_col from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("ShiftRight") {
      it("works") { testQuery("select ShiftRight(id, floor(critic_rating)) as t_col from movies") }
      it("udf in the left argument") {
        testQuery(
          "select ShiftRight(stringIdentity(id), floor(critic_rating)) as t_col from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the right argument") {
        testQuery(
          "select ShiftRight(id, stringIdentity(floor(critic_rating))) as t_col from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("ShiftRightUnsigned") {
      ignore("09/2024 - works") {
        testQuery("select ShiftRightUnsigned(id, floor(critic_rating)) as t_col from movies")
      }
      ignore("09/2024 - udf in the left argument") {
        testQuery(
          "select ShiftRightUnsigned(stringIdentity(id), floor(critic_rating)) as t_col from movies",
          expectPartialPushdown = true
        )
      }
      ignore("09/2024 - udf in the right argument") {
        testQuery(
          "select ShiftRightUnsigned(id, stringIdentity(floor(critic_rating))) as t_col from movies",
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("bit operations") {
    it("bit_count") { testQuery("SELECT bit_count(user_id) AS bit_count FROM reviews") }
    def bitOperationTest(sql: String): Unit = {
      val bitOperationsMinVersion = SinglestoreVersion(7, 0, 1)
      val resultSet               = spark.executeSinglestoreQuery("select @@memsql_version")
      val version                 = SinglestoreVersion(resultSet.next().getString(0))
      if (version.atLeast(bitOperationsMinVersion))
        testSingleReadForOldS2(sql, SinglestoreVersion(7, 6, 0))
    }
    it("bit_and") { bitOperationTest("SELECT bit_and(user_id) AS bit_and FROM reviews") }
    it("bit_and filter") {
      bitOperationTest("SELECT bit_and(user_id) filter (where user_id % 2 = 0) FROM reviews")
    }
    it("bit_or") { bitOperationTest("SELECT bit_or(age) AS bit_or FROM users") }
    it("bit_or filter") {
      bitOperationTest("SELECT bit_or(age) filter (where age % 2 = 0) FROM users")
    }
    it("bit_xor") { bitOperationTest("SELECT bit_xor(user_id) AS bit_xor FROM reviews") }
    it("bit_xor filter") {
      bitOperationTest("SELECT bit_xor(user_id) filter (where user_id % 2 = 0) FROM reviews")
    }
  }

  describe("datatypes") {
    // due to a bug in our dataframe comparison library we need to alias the column 4.9 to x...
    // this is because when the library asks spark for a column called "4.9", spark thinks the
    // library wants the table 4 and column 9.
    it("float literal") { testQuery("select 4.9 as x from movies") }

    it("negative float literal") { testQuery("select -24.345 as x from movies") }
    it("negative int literal") { testQuery("select -1 from users") }

    it("int") { testQuery("select id from users") }
    it("smallint") { testQuery("select age from users") }
    it("date") { testQuery("select birthday from users") }
    it("datetime") { testQuery("select created from reviews") }
    it("bool") { testQuery("select owns_house from users") }
    it("float") { testQuery("select critic_rating from movies") }
    it("text") { testQuery("select first_name from users") }

    it("typeof") {
      testSingleReadForReadFromLeaves(
        "SELECT typeof(user_id), typeof(created), typeof(review) FROM reviews")
    }
  }

  describe("filter") {
    it("numeric equality") { testQuery("select * from users where id = 1") }
    it("numeric inequality") { testQuery("select * from users where id != 1") }
    it("numeric comparison >") { testQuery("select * from users where id > 500") }
    it("numeric comparison > <") { testQuery("select * from users where id > 500 and id < 550") }
    it("string equality") { testQuery("select * from users where first_name = 'Evan'") }
  }

  describe("Aggregate Expressions") {
    describe("some aggregation functions") {
      val functions =
        Seq("skewness", "kurtosis", "var_pop", "var_samp", "stddev_samp", "stddev_pop", "avg")
      it("ints") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testSingleReadForOldS2(
            s"select $f (user_id) as t_col from reviews",
            SinglestoreVersion(7, 6, 0)
          )
        }
      }
      it("floats") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testSingleReadForOldS2(
            s"select $f (rating) as t_col from reviews",
            SinglestoreVersion(7, 6, 0)
          )
        }
      }
      it("floats with nulls") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testSingleReadForOldS2(
            s"select $f (critic_rating) as t_col from movies",
            SinglestoreVersion(7, 6, 0)
          )
        }
      }
      it("partial pushdown with udf") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testSingleReadQuery(
            s"select $f (stringIdentity(user_id)) as t_col from reviews",
            expectPartialPushdown = true
          )
        }
      }
      it("filter") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testSingleReadForOldS2(
            s"select $f(age) filter (where age % 2 = 0) from users",
            SinglestoreVersion(7, 6, 0)
          )
        }
      }
      it("filter for equal range population(std = 0)") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testSingleReadForOldS2(
            s"select $f(age) filter (where age = 60) from users",
            SinglestoreVersion(7, 6, 0)
          )
        }
      }
    }

    describe("min/max functions") {
      val functions = Seq("min", "max")
      it("ints") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testSingleReadForOldS2(
            s"select $f (user_id) as x from reviews",
            SinglestoreVersion(7, 6, 0)
          )
        }
      }
      it("floats") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testSingleReadForOldS2(
            s"select $f (rating) as x from reviews",
            SinglestoreVersion(7, 6, 0)
          )
        }
      }
      it("strings") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testSingleReadForOldS2(
            s"select $f (first_name) as x from users",
            SinglestoreVersion(7, 6, 0)
          )
        }
      }
      it("floats with nulls") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testSingleReadForOldS2(
            s"select $f (critic_rating) as x from movies",
            SinglestoreVersion(7, 6, 0)
          )
        }
      }
      it("partial pushdown with udf") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testSingleReadQuery(
            s"select $f (stringIdentity(user_id)) as x from reviews",
            expectPartialPushdown = true
          )
        }
      }
      it("filter") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testSingleReadForOldS2(
            s"select $f(age) filter (where age % 2 = 0) from users",
            SinglestoreVersion(7, 6, 0)
          )
        }
      }
    }

    describe("Sum") {
      // we cast the output, because SingleStore SUM returns DECIMAL(41, 0)
      // which is not supported by spark (spark maximum decimal precision is 38)
      it("ints") {
        testSingleReadForOldS2(
          "select cast(sum(age) as decimal(20, 0)) as x from users",
          SinglestoreVersion(7, 6, 0)
        )
      }
      it("floats") {
        testSingleReadForOldS2(
          "select sum(rating) as x from reviews",
          SinglestoreVersion(7, 6, 0)
        )
      }
      it("floats with nulls") {
        testSingleReadForOldS2(
          "select sum(critic_rating) as x from movies",
          SinglestoreVersion(7, 6, 0)
        )
      }
      it("partial pushdown with udf") {
        testSingleReadQuery(
          "select sum(stringIdentity(user_id)) as x from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("First") {
      ignore("09/2024 - succeeds") {
        testSingleReadForReadFromLeaves("select first(first_name) from users group by id")
      }
      ignore("09/2024 - partial pushdown with udf") {
        testSingleReadQuery(
          "select first(stringIdentity(first_name)) from users group by id",
          expectPartialPushdown = true
        )
      }
      ignore("09/2024 - filter") {
        testSingleReadForReadFromLeaves(
          "select first(first_name) filter (where age % 2 = 0) from users group by id"
        )
      }
    }

    describe("Last") {
      ignore("09/2024 - succeeds") {
        testSingleReadForReadFromLeaves("select last(first_name) from users group by id")
      }
      ignore("09/2024 - partial pushdown with udf") {
        testSingleReadQuery(
          "select last(stringIdentity(first_name)) from users group by id",
          expectPartialPushdown = true
        )
      }
      ignore("09/2024 - filter") {
        testSingleReadForReadFromLeaves(
          "select last(first_name) filter (where age % 2 = 0) from users group by id"
        )
      }
    }

    describe("Count") {
      it("all") {
        testSingleReadForOldS2("select count(*) from users", SinglestoreVersion(7, 6, 0))
      }
      it("distinct") {
        testSingleReadForOldS2(
          "select count(distinct first_name) from users",
          SinglestoreVersion(7, 6, 0)
        )
      }
      it("partial pushdown with udf (all)") {
        testSingleReadQuery(
          "select count(stringIdentity(first_name)) from users group by id",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown with udf (distinct)") {
        testSingleReadQuery(
          "select count(distinct stringIdentity(first_name)) from users group by id",
          expectPartialPushdown = true
        )
      }
      it("filter") {
        testSingleReadForOldS2(
          "select count(*) filter (where age % 2 = 0) from users",
          SinglestoreVersion(7, 6, 0)
        )
      }
      it("partial pushdown with udf in filter") {
        testSingleReadQuery(
          "SELECT count_if(age % 2 = 0) filter (where integerFilter(age)) FROM users",
          expectPartialPushdown = true
        )
      }
      it("count_if") {
        testSingleReadForOldS2(
          "SELECT count_if(age % 2 = 0) as count FROM users",
          SinglestoreVersion(7, 6, 0)
        )
      }
      it("count_if filter") {
        testSingleReadForOldS2(
          "SELECT count_if(age % 2 = 0) filter (where age % 2 = 0) FROM users",
          SinglestoreVersion(7, 6, 0)
        )
      }
    }
    it("top 3 email domains") {
      testOrderedQuery(
        """
          |select
          | domain,
          | count(*)
          |from (
          | select
          |   substring(email, locate('@', email) + 1) as domain
          | from users
          |)
          |group by 1
          |order by 2 desc, 1 asc
          |limit 3
          |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
      )
    }
  }

  describe("window functions") {
    it("rank order by") {
      testSingleReadForReadFromLeaves(
        "select out as a from (select rank() over (order by first_name) as out from users)"
      )
    }
    it("rank partition order by") {
      testSingleReadForReadFromLeaves(
        "select rank() over (partition by first_name order by first_name) as out from users"
      )
    }
    it("row_number order by") {
      testSingleReadForReadFromLeaves(
        "select row_number() over (order by first_name) as out from users"
      )
    }
    it("dense_rank order by") {
      testSingleReadForReadFromLeaves(
        "select dense_rank() over (order by first_name) as out from users"
      )
    }
    it("lag order by") {
      testSingleReadForReadFromLeaves(
        "select first_name, lag(first_name) over (order by first_name) as out from users"
      )
    }
    it("lead order by") {
      testSingleReadForReadFromLeaves(
        "select first_name, lead(first_name) over (order by first_name) as out from users"
      )
    }
    it("ntile(3) order by") {
      testSingleReadForReadFromLeaves(
        "select first_name, ntile(3) over (order by first_name) as out from users"
      )
    }
    it("percent_rank order by") {
      testSingleReadForReadFromLeaves(
        "select first_name, percent_rank() over (order by first_name) as out from users"
      )
    }
  }

  describe("sort/limit") {
    it("numeric order") {
      testOrderedQuery("select * from users order by id asc")
    }
    it("text order") {
      testOrderedQuery("select * from users order by first_name desc, last_name asc, id asc")
    }
    it("text order expression") {
      testOrderedQuery("select * from users order by `email` like '%@gmail%', id asc")
    }
    it("text order case") {
      testOrderedQuery(
        "select * from users where first_name in ('Abbey', 'a') order by first_name desc, id asc"
      )
    }
    it("simple limit") { testSingleReadForReadFromLeaves("select 'a' from users limit 10") }
    it("limit with sort") { testOrderedQuery("select * from users order by id limit 10") }
    it("limit with sort on inside") {
      testOrderedQuery("select * from (select * from users order by id) limit 10")
    }
    it("limit with sort on outside") {
      testOrderedQuery(
        """
          |select *
          |from (
          | select id
          | from (
          |   select id, email
          |   from users
          |   order by id
          |   limit 10
          | )
          | limit 100
          |)
          |order by id
          |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
      )
    }
  }

  describe("hashes") {
    describe("sha1") {
      it("works with text") { testQuery("select sha1(first_name) from users") }
      it("works with null") { testSingleReadForReadFromLeaves("select sha1(null) from users") }
      it("partial pushdown") {
        testQuery(
          """
            |select
            | sha1(stringIdentity(first_name)) as sha1,
            | stringIdentity(first_name) as first_name
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    describe("sha2") {
      it("0 bit length") { testQuery("select sha2(first_name, 0) from users") }
      it("256 bit length") { testQuery("select sha2(first_name, 256) from users") }
      it("384 bit length") { testQuery("select sha2(first_name, 384) from users") }
      it("512 bit length") { testQuery("select sha2(first_name, 512) from users") }
      it("works with null") { testSingleReadForReadFromLeaves("select sha2(null, 256) from users") }
      it("224 bit length partial pushdown") {
        testQuery("select sha2(first_name, 224) from users", expectPartialPushdown = true)
      }
      it("partial pushdown") {
        testQuery(
          """
            |select
            | sha2(stringIdentity(first_name), 256) as sha2,
            | stringIdentity(first_name) as first_name
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
      it("short literal") { testQuery(s"select sha2(first_name, 256S) from users") }
      it("long literal") { testQuery(s"select sha2(first_name, 256L) from users") }
    }

    describe("md5") {
      it("works with text") { testQuery("select md5(first_name) from users") }
      it("works with null") { testSingleReadForReadFromLeaves("select md5(null) from users") }
      it("partial pushdown") {
        testQuery(
          """
            |select
            | md5(stringIdentity(first_name)) as md5,
            | stringIdentity(first_name) as first_name
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("joins") {
    describe("successful pushdown") {
      it("implicit inner join") {
        testSingleReadForReadFromLeaves(
          "select * from users as a, reviews where a.id = reviews.user_id"
        )
      }
      it("explicit inner join") {
        testSingleReadForReadFromLeaves(
          "select * from users inner join reviews on users.id = reviews.user_id"
        )
      }
      it("cross join") {
        testSingleReadForReadFromLeaves(
          "select * from users cross join reviews on users.id = reviews.user_id"
        )
      }
      it("cross join with sort and limit") {
        testSingleReadForReadFromLeaves(
          """
            |select *
            |from
            | (select * from users order by id limit 10) as users
            | cross join
            | (select * from reviews order by user_id limit 10) as reviews
            | on users.id = reviews.user_id
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("left outer join") {
        testSingleReadForReadFromLeaves(
          "select * from users left outer join reviews on users.id = reviews.user_id"
        )
      }
      it("right outer join") {
        testSingleReadForReadFromLeaves(
          "select * from users right outer join reviews on users.id = reviews.user_id"
        )
      }
      it("full outer join") {
        testSingleReadForReadFromLeaves(
          "select * from users full outer join reviews on users.id = reviews.user_id"
        )
      }
      it("natural join") {
        testSingleReadForReadFromLeaves(
          "select users.id, rating from users natural join (select user_id as id, rating from reviews)"
        )
      }
      it("complex join") {
        testSingleReadForReadFromLeaves(
          """
            |select
            | users.id,
            | round(avg(rating), 2) as rating,
            | count(*) as num_reviews
            |from
            | users
            | inner join
            | reviews
            | on users.id = reviews.user_id
            |group by users.id
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("inner join without condition") {
        testOrderedQuery(
          """
            |select *
            |from
            | users
            | inner join
            | reviews
            |order by concat(users.id, ' ', reviews.user_id, ' ', reviews.movie_id)
            |limit 10
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("cross join without condition") {
        testOrderedQuery(
          """
            |select *
            |from
            | users
            | cross join
            | reviews
            |order by concat(users.id, ' ', reviews.user_id, ' ', reviews.movie_id)
            |limit 10
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
    }

    describe("unsuccessful pushdown") {
      describe("udf in the left relation") {
        it("explicit inner join") {
          testSingleReadQuery(
            """
              |select *
              |from
              | (select rating, stringIdentity(user_id) as user_id from reviews)
              | inner join
              | users
              | on users.id = user_id
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
        it("cross join") {
          testSingleReadQuery(
            """
              |select *
              |from
              | (select rating, stringIdentity(user_id) as user_id from reviews)
              | cross join
              | users
              | on users.id = user_id
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
        it("left outer join") {
          testSingleReadQuery(
            """
              |select *
              |from
              | (select rating, stringIdentity(user_id) as user_id from reviews)
              | left outer join
              | users
              | on users.id = user_id
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
        it("right outer join") {
          testSingleReadQuery(
            """
              |select *
              |from
              | (select rating, stringIdentity(user_id) as user_id from reviews)
              | right outer join
              | users
              | on users.id = user_id
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
        it("full outer join") {
          testSingleReadQuery(
            """
              |select *
              |from
              | (select rating, stringIdentity(user_id) as user_id from reviews)
              | full outer join
              | users
              | on users.id = user_id
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
      }

      describe("udf in the right relation") {
        it("explicit inner join") {
          testSingleReadQuery(
            """
              |select *
              |from
              | users
              | inner join
              | (select rating, stringIdentity(user_id) as user_id from reviews)
              | on users.id = user_id
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
        it("cross join") {
          testSingleReadQuery(
            """
              |select *
              |from
              | users
              | cross join
              | (select rating, stringIdentity(user_id) as user_id from reviews)
              | on users.id = user_id
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
        it("left outer join") {
          testSingleReadQuery(
            """
              |select *
              |from
              | users
              | left outer join
              | (select rating, stringIdentity(user_id) as user_id from reviews)
              | on users.id = user_id
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
        it("right outer join") {
          testSingleReadQuery(
            """
              |select *
              |from
              | users
              | right outer join
              | (select rating, stringIdentity(user_id) as user_id from reviews)
              | on users.id = user_id
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
        it("full outer join") {
          testSingleReadQuery(
            """
              |select *
              |from
              | users
              | full outer join
              | (select rating, stringIdentity(user_id) as user_id from reviews)
              | on users.id = user_id
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
      }

      describe("udf in the condition") {
        it("explicit inner join") {
          testSingleReadQuery(
            """
              |select *
              |from
              | users
              | inner join
              | reviews
              | on stringIdentity(users.id) = stringIdentity(reviews.user_id)
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
        it("cross join") {
          testSingleReadQuery(
            """
              |select *
              |from
              | users
              | cross join
              | reviews
              | on stringIdentity(users.id) = stringIdentity(reviews.user_id)
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
        it("left outer join") {
          testSingleReadQuery(
            """
              |select *
              |from
              | users
              | left outer join
              | reviews
              | on stringIdentity(users.id) = stringIdentity(reviews.user_id)
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
        it("right outer join") {
          testSingleReadQuery(
            """
              |select *
              |from
              | users
              | right outer join
              | reviews
              | on stringIdentity(users.id) = stringIdentity(reviews.user_id)
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
        it("full outer join") {
          testSingleReadQuery(
            """
              |select *
              |from
              | users
              | full outer join
              | reviews
              | on stringIdentity(users.id) = stringIdentity(reviews.user_id)
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
      }

      describe("outer joins with empty condition") {
        it("left outer join") {
          testQuery(
            "select * from users left outer join (select rating from reviews order by rating limit 10)",
            expectPartialPushdown = true
          )
        }
        it("right outer join") {
          testSingleReadQuery(
            """
              |select *
              |from
              | users
              | right outer join
              | (select rating from reviews order by rating limit 10)
              |order by age
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
        it("full outer join") {
          testQuery(
            "select * from users full outer join (select rating from reviews order by rating limit 10)",
            expectPartialPushdown = true
          )
        }
      }

      describe("different dml jdbc options") {
        def testPushdown(joinType: String): Unit = {
          val df1 =
            spark.read
              .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
              .options(Map("dmlEndpoint" -> "host1:1020,host2:1010"))
              .load("testdb.users")
          val df2 =
            spark.read
              .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
              .options(Map("dmlEndpoint" -> "host3:1020,host2:1010"))
              .load("testdb.reviews")

          val joinedDf = df1.join(df2, df1("id") === df2("user_id"), joinType)
          log.debug(joinedDf.queryExecution.optimizedPlan.toString())
          assert(
            joinedDf.queryExecution.optimizedPlan match {
              case SQLGen.Relation(_) => false
              case _                  => true
            },
            "Join of the relations with different jdbc connection options should not be pushed down"
          )
        }

        it("explicit inner join") { testPushdown("inner") }
        it("cross join") {  testPushdown("cross") }
        it("left outer join") { testPushdown("leftouter") }
        it("right outer join") { testPushdown("rightouter") }
        it("full outer join") { testPushdown("fullouter") }
      }
    }
  }

  describe("predicates") {
    describe("and") {
      it("works with cast and true") {
        testQuery("select cast(owns_house as boolean) and true from users")
      }
      it("works with cast and false") {
        testQuery("select cast(owns_house as boolean) and false from users")
      }
      it("works with cast to bool") {
        testQuery("select cast(id as boolean) and cast(owns_house as boolean) from users")
      }
      it("works with true and false") {
        testQuery("select true and false from users")
      }
      it("works with null") {
        testQuery("select cast(id as boolean) and null from users")
      }
      it("partial pushdown") {
        testQuery(
          """
            |select
            | cast(stringIdentity(id) as boolean) and cast(stringIdentity(owns_house) as boolean)
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    describe("or") {
      it("works with cast or true") {
        testQuery("select cast(owns_house as boolean) or true from users")
      }
      it("works with cast or false") {
        testQuery("select cast(owns_house as boolean) or false from users")
      }
      it("works with cast to bool") {
        testQuery("select cast(id as boolean) or cast(owns_house as boolean) from users")
      }
      it("works with true or false") {
        testQuery("select true or false from users")
      }
      it("works with null") {
        testQuery("select cast(id as boolean) or null from users")
      }
      it("partial pushdown") {
        testQuery(
          """
            |select
            | cast(stringIdentity(id) as boolean) or cast(stringIdentity(owns_house) as boolean)
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    describe("equal") {
      it("works with tinyint") {
        testQuery("select owns_house = 1 as owns_house from users")
      }
      it("works with single brackets") {
        testQuery("select id = '1' as owns_house from users")
      }
      it("works with boolean") {
        testQuery("select cast(owns_house as boolean) = true as owns_house from users")
      }
      it("works with tinyint equals null") {
        testQuery("select owns_house = null as owns_house from users")
      }
      it("works with null equals null") {
        testQuery("select null = null as null from users")
      }
      it("partial pushdown") {
        testQuery(
          "select stringIdentity(id) = '1' from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("equalNullSafe") {
      it("works with tinyint") {
        testQuery("select owns_house <=> 1 as owns_house from users")
      }
      it("works with single brackets") {
        testQuery("select id <=> '1' as owns_house from users")
      }
      it("works with boolean") {
        testQuery("select cast(owns_house as boolean) <=> true as owns_house from users")
      }
      it("works with tinyint equals null") {
        testQuery("select owns_house <=> null as owns_house from users")
      }
      it("works with null equals null") {
        testQuery("select null <=> null as null from users")
      }
      it("partial pushdown") {
        testQuery(
          "select stringIdentity(id) <=> '1' from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("lessThan") {
      it("works with tinyint") {
        testQuery("select owns_house < 1 as owns_house from users")
      }
      it("works with single brackets") {
        testQuery("select id < '10' as owns_house from users")
      }
      it("works with text") {
        testQuery("select first_name < 'ZZZ' as first_name from users")
      }
      it("works with boolean") {
        testQuery("select cast(owns_house as boolean) < true as owns_house from users")
      }
      it("works with tinyint less than null") {
        testQuery("select owns_house < null as owns_house from users")
      }
      it("works with null less than null") {
        testQuery("select null < null as null from users")
      }
      it("partial pushdown") {
        testQuery(
          "select stringIdentity(id) < '10' from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("lessThanOrEqual") {
      it("works with tinyint") {
        testQuery("select owns_house <= 1 as owns_house from users")
      }
      it("works with single brackets") {
        testQuery("select id <= '10' as owns_house from users")
      }
      it("works with text") {
        testQuery("select first_name <= 'ZZZ' as first_name from users")
      }
      it("works with boolean") {
        testQuery("select cast(owns_house as boolean) <= true as owns_house from users")
      }
      it("works with tinyint less than or equal null") {
        testQuery("select owns_house <= null as owns_house from users")
      }
      it("works with null less than or equal null") {
        testQuery("select null <= null as null from users")
      }
      it("partial pushdown") {
        testQuery(
          "select stringIdentity(id) <= '10' from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("greaterThan") {
      it("works with tinyint") {
        testQuery("select owns_house > 1 as owns_house from users")
      }
      it("works with single brackets") {
        testQuery("select id > '10' as owns_house from users")
      }
      it("works with text") {
        testQuery("select first_name > 'ZZZ' as first_name from users")
      }
      it("works with boolean") {
        testQuery("select cast(owns_house as boolean) > false as owns_house from users")
      }
      it("works with tinyint greater than null") {
        testQuery("select owns_house > null as owns_house from users")
      }
      it("works with null greater than null") {
        testQuery("select null > null as null from users")
      }
      it("partial pushdown") {
        testQuery(
          "select stringIdentity(id) > '10' from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("greaterThanOrEqual") {
      it("works with tinyint") {
        testQuery("select owns_house >= 1 as owns_house from users")
      }
      it("works with single brackets") {
        testQuery("select id >= '10' as owns_house from users")
      }
      it("works with text") {
        testQuery("select first_name >= 'ZZZ' as first_name from users")
      }
      it("works with boolean") {
        testQuery("select cast(owns_house as boolean) >= true as owns_house from users")
      }
      it("works with tinyint greater than or equal null") {
        testQuery("select owns_house >= null as owns_house from users")
      }
      it("works with null greater than or equal null") {
        testQuery("select null >= null as null from users")
      }
      it("partial pushdown") {
        testQuery(
          "select stringIdentity(id) >= '10' from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("in") {
      it("works with tinyint") {
        testQuery("select owns_house in(1) as owns_house from users")
      }
      it("works with single brackets") {
        testQuery("select id in('10','11','12') as owns_house from users")
      }
      it("works with text") {
        testQuery("select first_name in('Wylie', 'Sukey', 'Sondra') as first_name from users")
      }
      it("works with boolean") {
        testQuery("select cast(owns_house as boolean) in(true) as owns_house from users")
      }
      it("works with tinyint in null") {
        testQuery("select owns_house in(null) as owns_house from users")
      }
      it("works with null in null") {
        testQuery("select null in(null) as null from users")
      }
      it("partial pushdown") {
        testQuery(
          "select stringIdentity(id) in('10') from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("not") {
      it("works with tinyint") {
        testQuery("select not (owns_house = 1) as not_owns_house from users")
      }
      it("works with single brackets") {
        testQuery("select not (id = '10') as owns_house from users")
      }
      it("works with text") {
        testQuery("select not (first_name = 'Wylie') as first_name from users")
      }
      it("works with boolean") {
        testQuery("select not (cast(owns_house as boolean) = true) as owns_house from users")
      }
      it("works with tinyint not null") {
        testQuery("select not (owns_house = null) as owns_house from users")
      }
      it("works with not null") {
        testQuery("select not (null = null) as null from users")
      }
      it("partial pushdown") {
        testQuery(
          "select not (stringIdentity(id) = '10') from users",
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("same-name column selection") {
    it("join two tables which project the same column name") {
      testOrderedQuery(
        """
          |select *
          |from
          | (select id from users) as a,
          | (select id from movies) as b
          |where a.id = b.id
          |order by a.id
          |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
      )
    }
    it("select same columns twice via natural join") {
      testOrderedQuery("select * from users as a natural join users order by a.id")
    }
    it("select same column twice from table") {
      testQuery(
        "select first_name, first_name from users",
        expectPartialPushdown = true
      )
    }
    it("select same column twice from table with aliases") {
      testOrderedQuery("select first_name as a, first_name as a from users order by id")
    }
    it("select same alias twice (different column) from table") {
      testOrderedQuery("select first_name as a, last_name as a from users order by id")
    }
    it("select same column twice in subquery") {
      testQuery(
        "select * from (select first_name, first_name from users) as x",
        expectPartialPushdown = true
      )
    }
    it("select same column twice from subquery with aliases") {
      testOrderedQuery(
        """
          |select *
          |from
          | (select first_name as a, first_name as a from users order by id) as t_table
          |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
      )
    }
  }

  describe("datetimeExpressions") {
    describe("DateAdd") {
      it("positive num_days") { testQuery("select date_add(birthday, age) from users") }
      it("negative num_days") { testQuery("select date_add(birthday, -age) from users") }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          "select date_add(stringIdentity(birthday), age) from users",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          "select date_add(birthday, -cast(stringIdentity(age) as int)) from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("DateSub") {
      it("positive num_days") { testQuery("select date_sub(birthday, age) from users") }
      it("negative num_days") { testQuery("select date_sub(birthday, -age) from users") }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          "select date_sub(stringIdentity(birthday), age) from users",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          "select date_sub(birthday, -cast(stringIdentity(age) as int)) from users",
          expectPartialPushdown = true
        )
      }
    }

    val intervals = List(
      "1 year",
      "1 month",
      "3 week",
      "2 day",
      "7 hour",
      "3 minute",
      "5 second",
      "1 year 1 month",
      "1 week 3 hour 5 minute 4 seconds"
    )

    describe("toUnixTimestamp") {
      it("works with TimestampType") {
        testQuery("select created, to_unix_timestamp(created) from reviews")
      }
      it("works with DateType") {
        testQuery("select birthday, to_unix_timestamp(birthday) from users")
      }
      it("partial pushdown because of udf") {
        testQuery(
          "select to_unix_timestamp(stringIdentity(birthday)) from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("unixTimestamp") {
      it("works with TimestampType") {
        testQuery("select created, unix_timestamp(created) from reviews")
      }
      it("works with DateType") {
        testQuery("select birthday, unix_timestamp(birthday) from users")
      }
      it("partial pushdown because of udf") {
        testQuery(
          "select unix_timestamp(stringIdentity(birthday)) from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("fromUnixTime") {
      it("works") {
        // cast is needed because in SingleStore 6.8 FROM_UNIXTIME query returns a result with microseconds
        testQuery("select id, cast(from_unixtime(id) as timestamp) from movies")
      }
      it("tutu") {
        testQuery(
          "select from_unixtime(stringIdentity(id)) from movies",
          expectPartialPushdown = true
        )
      }
    }

    // SingleStore and Spark differ on how they do last day calculations, so we ignore
    // them in some of these tests

    describe("timeAdd") {
      it("works") {
        for (interval <- intervals) {
          println(s"testing timeAdd with interval $interval")
          val query =
            s"""
              |select
              | created,
              | created + interval $interval
              |from reviews
              |where date(created) != last_day(created)
              |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
          if (!interval.contains("day") || spark.version != "3.0.0") {
            testQuery(query)
          }
        }
      }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          s"""
            |select
            | created,
            | to_timestamp(stringIdentity(created)) + interval 1 month
            |from reviews
            |where date(created) != last_day(created)
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    describe("timeSub") {
      it("works") {
        for (interval <- intervals) {
          println(s"testing timeSub with interval $interval")
          testQuery(
            s"""
              |select
              |  created,
              |  created - interval $interval
              |from reviews
              |where date(created) != last_day(created)
              |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
          )
        }
      }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          s"""
            |select
            | created,
            | to_timestamp(stringIdentity(created)) - interval 1 day
            |from reviews
            |where date(created) != last_day(created)
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    describe("addMonths") {
      it("works") {
        val numMonthsList = List(0, 1, 2, 12, 13, 200, -1, -2, -12, -13, -200)
        for (numMonths <- numMonthsList) {
          println(s"testing addMonths with $numMonths months")
          testQuery(
            s"""
              |select
              | created,
              | add_months(created, $numMonths)
              |from reviews
              |where date(created) != last_day(created)
              |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
          )
        }
      }
      it("partial pushdown with udf in the left argument") {
        testQuery(
          s"""
            |select
            | created,
            | add_months(stringIdentity(created), 1)
            |from reviews
            |where date(created) != last_day(created)
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
      it("partial pushdown with udf in the right argument") {
        testQuery(
          s"""
            |select
            | created,
            | add_months(created, stringIdentity(1))
            |from reviews
            |where date(created) != last_day(created)
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    describe("NextDay") {
      it("works") {
        for ((dayOfWeek, _) <- ExpressionGen.DAYS_OF_WEEK_OFFSET_MAP) {
          println(s"testing nextDay with $dayOfWeek")
          testQuery(
            s"""
              |select
              | created,
              | next_day(created, '$dayOfWeek')
              |from reviews
              |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
          )
        }
      }
      it("works with invalid day name") {
        testQuery(
          s"""
            |select
            | created,
            | next_day(created, 'invalid_day')
            |from reviews
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          "select next_day(stringIdentity(created), 'monday') from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          "select next_day(created, stringIdentity('monday')) from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("DateDiff") {
      it("works") {
        testSingleReadForReadFromLeaves(
          """
            |select
            | birthday,
            | created,
            | DateDiff(birthday, created),
            | DateDiff(created, birthday),
            | DateDiff(created, created)
            |from
            | users
            | inner join
            | reviews
            | on users.id = reviews.user_id
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          "select DateDiff(stringIdentity(created), created) from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          "select DateDiff(created, stringIdentity(created)) from reviews",
          expectPartialPushdown = true
        )
      }
    }

    // Spark doesn't support explicit time intervals like `+/-hh:mm`

    val timeZones = List(
      "US/Mountain",
      "Asia/Seoul",
      "UTC",
      "EST",
      "Etc/GMT-6"
    )

    describe("fromUTCTimestamp") {
      it("works") {
        for (timeZone <- timeZones) {
          println(s"testing fromUTCTimestamp with timezone $timeZone")
          testQuery(s"select from_utc_timestamp(created, '$timeZone') from reviews")
        }
      }
      it("partial pushdown because of udf in the left argument") {
        testQuery("select from_utc_timestamp(stringIdentity(created), 'EST') from reviews",
                  expectPartialPushdown = true)
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          "select from_utc_timestamp(created, stringIdentity('EST')) from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("toUTCTimestamp") {
      it("works") {
        for (timeZone <- timeZones) {
          println(s"testing toUTCTimestamp with timezone $timeZone")
          // singlestore doesn't support timestamps less then 1970-01-01T00:00:00Z
          testQuery(
            s"""
              |select
              | to_utc_timestamp(created, '$timeZone')
              |from reviews
              |where to_unix_timestamp(created) > 24*60*60
              |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
          )
        }
      }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          "select to_utc_timestamp(stringIdentity(created), 'EST') from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          "select to_utc_timestamp(created, stringIdentity('EST')) from reviews",
          expectPartialPushdown = true
        )
      }
    }

    // TruncTimestamp is called as date_trunc() in Spark
    describe("truncTimestamp") {
      it("works") {
        val dateParts = List(
          "YEAR",
          "YYYY",
          "YY",
          "MON",
          "MONTH",
          "MM",
          "DAY",
          "DD",
          "HOUR",
          "MINUTE",
          "SECOND",
          "WEEK",
          "QUARTER"
        )
        for (datePart <- dateParts) {
          println(s"testing truncTimestamp with datepart $datePart")
          testQuery(s"select date_trunc('$datePart', created) from reviews")
        }
      }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          s"select date_trunc(stringIdentity('DAY'), created) from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          s"select date_trunc('DAY', stringIdentity(created)) from reviews",
          expectPartialPushdown = true
        )
      }
    }

    // TruncDate is called as trunc()
    describe("truncDate") {
      it("works") {
        val dateParts = List("YEAR", "YYYY", "YY", "MON", "MONTH", "MM")
        for (datePart <- dateParts) {
          println(s"testing truncDate with datepart $datePart")
          testQuery(s"select trunc(created, '$datePart') from reviews")
        }
      }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          s"select trunc(stringIdentity(created), 'MONTH') from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          s"select trunc(created, stringIdentity('MONTH')) from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("monthsBetween") {
      it("works") {
        for (interval <- intervals) {
          println(s"testing monthsBetween with interval $interval")
          testQuery(
            s"select months_between(created, created + interval $interval) from reviews"
          )
        }
      }
      it("partial pushdown because of udf in the left argument") {
        testQuery(
          s"select months_between(stringIdentity(created), created + interval 1 month) from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument") {
        testQuery(
          s"""
             |select
             | months_between(created, to_timestamp(stringIdentity(created)) + interval 1 month)
             |from reviews
             |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    val periodsList: List[List[String]] = List(
      List("YEAR", "Y", "YEARS", "YR", "YRS"),
      List("QUARTER", "QTR"),
      List("MONTH", "MON", "MONS", "MONTHS"),
      List("WEEK", "W", "WEEKS"),
      List("DAY", "D", "DAYS"),
      List("DAYOFWEEK", "DOW"),
      List("DAYOFWEEK_ISO", "DOW_ISO"),
      List("DOY"),
      List("HOUR", "H", "HOURS", "HR", "HRS"),
      List("MINUTE", "MIN", "M", "MINS", "MINUTES")
    )

    it("extract") {
      for (periods <- periodsList) {
        for (period <- periods) {
          testQuery(s"SELECT extract($period FROM birthday) as extract_period from users")
          testQuery(s"SELECT extract($period FROM created) as extract_period from reviews")
        }
      }
    }

    it("datePart") {
      for (periods <- periodsList) {
        for (period <- periods) {
          testQuery(s"SELECT date_part('$period', birthday) as date_part from users")
          testQuery(s"SELECT date_part('$period', created) as date_part from reviews")
        }
      }
    }

    it("makeDate") {
      testQuery("SELECT make_date(1000, user_id, user_id) FROM reviews")
    }

    it("makeTimestamp") {
      testQuery(
        "SELECT make_timestamp(1000, user_id, user_id, user_id, user_id, user_id) FROM reviews"
      )
    }

    it("CurrentDate") {
      testQuery("select current_date() from users", expectSameResult = false)
    }
    it("Now") {
      testQuery("select now() from users", expectSameResult = false)
    }
    it("DateFromUnixDate") {
      testQuery("select date_from_unix_date(1234567) from users")
    }
    it("CurrentTimestamp") {
      testQuery("select current_timestamp() from users", expectSameResult = false)
    }

    describe("UnixDate") {
      it("Simple query, one day after epoch") {
        testQuery("select unix_date(date('1970-01-02')) from users")
      }
      it("works") {
        testQuery("select unix_date(birthday) from users")
      }
      it("partial pushdown") {
        testQuery(
          "select unix_date(date(stringIdentity(birthday))) from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("unix seconds functions") {
      val functions = Seq("unix_seconds", "unix_micros", "unix_millis")

      it("works with timestamp") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(s"select $f(created) from reviews")
        }
      }
      it("partial pushdown") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(s"select $f(timestamp(stringIdentity(created))) from reviews",
                    expectPartialPushdown = true)
        }
      }
    }

    describe("converting (milli/micro)seconds to timestamp functions") {
      val functions = Seq("timestamp_seconds", "timestamp_millis", "timestamp_micros")

      it("works") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(s"select created, $f(user_id) from reviews")
        }
      }
      it("works with negative data") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(s"select $f(-movie_id) from reviews")
        }
      }
      it("partial pushdown") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(
            s"select $f(int(stringIdentity(id))) from users",
            expectPartialPushdown = true
          )
        }
      }
    }

    describe("timestamp parts functions") {
      val functions = Seq(
        "Hour",
        "Minute",
        "Second",
        "DayOfYear",
        "Year",
        "Quarter",
        "Month",
        "DayOfMonth",
        "DayOfWeek",
        "WeekOfYear",
        "last_day"
      )

      it("works with date") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(s"select $f(birthday) from users")
        }
      }
      it("works with timestamp") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(s"select $f(created) from reviews")
        }
      }
      it("works with string") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(s"select $f(first_name) from users")
        }
      }
      it("partial pushdown") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(
            s"select $f(stringIdentity(first_name)) from users",
            expectPartialPushdown = true
          )
        }
      }
    }
  }

  describe("partial pushdown") {
    it("ignores spark UDFs") {
      testQuery(
        "select stringUpper(first_name), id from users where id in (10,11,12)",
        expectPartialPushdown = true
      )
    }
    it("join with pure-jdbc relation") {
      testSingleReadQuery(
        """
          |select
          | users.id,
          | concat(first(users.first_name), " ", first(users.last_name)) as full_name
          |from
          | users
          | inner join testdb_jdbc.reviews
          | on users.id = reviews.user_id
          |group by users.id
          |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
        expectPartialPushdown = true
      )
    }
  }

  describe("stringExpressions") {
    describe("StartsWith") {
      it("works") {
        testQuery(
          "select * from movies",
          filterDF = df => df.filter(df.col("critic_review").startsWith("M"))
        )
      }
      it("udf in the left argument") {
        testQuery(
          "select stringIdentity(critic_review) as x from movies",
          filterDF = df => df.filter(df.col("x").startsWith("M")),
          expectPartialPushdown = true
        )
      }
    }

    describe("EndsWith") {
      it("works") {
        testQuery(
          "select * from movies",
          filterDF = df => df.filter(df.col("critic_review").endsWith("s."))
        )
      }
      it("udf in the left argument") {
        testQuery(
          "select stringIdentity(critic_review) as x from movies",
          filterDF = df => df.filter(df.col("x").endsWith("s.")),
          expectPartialPushdown = true
        )
      }
    }

    describe("Contains") {
      it("works") {
        testQuery(
          "select * from movies",
          filterDF = df => df.filter(df.col("critic_review").contains("a"))
        )
      }
      it("udf in the left argument") {
        testQuery(
          "select stringIdentity(critic_review) as x from movies",
          filterDF = df => df.filter(df.col("x").contains("a")),
          expectPartialPushdown = true
        )
      }
    }

    describe("StringInstr") {
      it("works") {
        testQuery("select instr(critic_review, 'id') from movies")
      }
      it("works when all arguments are not literals") {
        testQuery("select instr(critic_review, critic_review) from movies")
      }
      it("udf in the left argument") {
        testQuery(
          "select instr(stringIdentity(critic_review), 'id') from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the right argument") {
        testQuery(
          "select instr(critic_review, stringIdentity('id')) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("FindInSet") {
      it("works") {
        testQuery("select id, find_in_set(id, '82, 1,13,54,28,39,42, owns_house,120') from users")
      }
      it("constant example") {
        testQuery("select id, find_in_set('39', '1,2,3990, 13,28,39,42,54,82,120') from users")
      }
      it("works with empty left argument") {
        testQuery("select find_in_set('', '1,2,3') from users")
      }
      it("works with empty right argument") {
        testQuery("select find_in_set(id, '') from users")
      }
      it("udf in the left argument") {
        testQuery(
          "select find_in_set(stringIdentity(critic_review), 'id') from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the right argument") {
        testQuery(
          "select find_in_set(critic_review, stringIdentity('id')) from movies",
          expectPartialPushdown = true
        )
      }
      it("joinable object in the right argument") {
        testQuery(
          "select find_in_set(critic_review, id) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("StringTrim") {
      it("works") {
        testQuery("select id, trim(first_name) as t_col from users")
      }
      it("works (other syntax)") {
        testQuery("select id, btrim(first_name) as t_col from users")
      }
      it("works when trimStr is ' '") {
        testQuery("select id, trim(both ' ' from first_name) as t_col from users")
      }
      it("works when trimStr is ' ' (other syntax)") {
        testQuery("select id, trim(' ', first_name) as t_col from users")
      }
      it("works when trimStr is not None and not ' '") {
        testQuery("select id, trim(both '@' from first_name) as t_col from users")
      }
      it("works when trimStr is not None and not ' ' (other syntax)") {
        testQuery("select id, trim('@', first_name) as t_col from users")
      }
      it("partial pushdown with udf") {
        testQuery(
          "select id, trim(stringIdentity(first_name)) from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("StringTrimLeft") {
      it("works") {
        testQuery("select id, ltrim(first_name) as t_col from users")
      }
      it("works when trimStr is ' '") {
        testQuery("select id, trim(leading ' ' from first_name) as t_col from users")
      }
      it("works when trimStr is ' ' (other syntax)") {
        testQuery("select id, ltrim(' ', first_name) as t_col from users")
      }
      it("works when trimStr is not None and not ' '") {
        testQuery("select id, trim(leading '@' from first_name) as t_col from users")
      }
      it("works when trimStr is not None and not ' ' (other syntax)") {
        testQuery("select id, ltrim('@', first_name) as t_col from users")
      }
      it("partial pushdown with udf") {
        testQuery(
          "select id, ltrim(stringIdentity(first_name)) from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("StringTrimRight") {
      it("works") {
        testQuery("select id, rtrim(first_name) as t_col from users")
      }
      it("works when trimStr is ' '") {
        testQuery("select id, trim(trailing ' ' from first_name) as t_col from users")
      }
      it("works when trimStr is ' ' (other syntax)") {
        testQuery("select id, rtrim(' ', first_name) as t_col from users")
      }
      it("works when trimStr is not None and not ' '") {
        testQuery("select id, trim(trailing '@' from first_name) as t_col from users")
      }
      it("works when trimStr is not None and not ' ' (other syntax)") {
        testQuery("select id, rtrim('@', first_name) as t_col from users")
      }
      it("partial pushdown with udf") {
        testQuery(
          "select id, rtrim(stringIdentity(first_name)) from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("FormatNumber") {
      // singlestore and spark rounds fractional types differently
      it("works with zero precision") {
        testQuery(
          """
            |select
            | format_number(critic_rating, 0)
            |from movies
            |where critic_rating - floor(critic_rating) != 0.5 or critic_rating is null
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("works with negative precision") {
        testQuery(
          """
            |select
            | format_number(critic_rating, -10)
            |from movies
            |where critic_rating - floor(critic_rating) != 0.5 or critic_rating is null
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("works with numbers and zero precision") {
        testQuery(
          """
            |select
            | format_number(id, 0)
            |from movies
            |where critic_rating - floor(critic_rating) != 0.5 or critic_rating is null
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("works with numbers and negative precision") {
        testQuery(
          """
            |select
            | format_number(id, -10)
            |from movies
            |where critic_rating - floor(critic_rating) != 0.5 or critic_rating is null
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("works with positive precision") {
        testQuery(
          """
            |select
            | format_number(critic_rating, cast(floor(critic_rating) as int)) as t_col
            |from movies
            |where
            |(
            | critic_rating - floor(critic_rating) != 0.5 and
            | critic_rating*pow(10, floor(critic_rating)) - floor(critic_rating*pow(10, floor(critic_rating))) != 0.5
            |) or critic_rating is null
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("works with negative numbers") {
        testQuery(
          """
            |select
            | format_number(-critic_rating, 0), round(critic_rating, 5) as t_col
            |from movies
            |where
            |(
            | critic_rating - floor(critic_rating) != 0.5 and abs(critic_rating) > 1
            |) or critic_rating is null
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it(
        "works with null",
        ExcludeFromSpark31,
        ExcludeFromSpark32,
        ExcludeFromSpark33,
        ExcludeFromSpark34,
        ExcludeFromSpark35
      ) {
        // in 3.1 version, spark simplifies this query and doesn't send
        // it to the database, so it is read from single partition
        testQuery(
          """
            |select
            | format_number(critic_rating, null)
            |from movies
            |where critic_rating - floor(critic_rating) != 0.5 or critic_rating is null
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("works with format") {
        if (spark.version != "2.3.4") {
          testQuery(
            "select format_number(critic_rating, '#####,#,#,#.##') as x from movies",
            expectPartialPushdown = true
          )
        }
      }
      it("udf in the left argument") {
        testQuery(
          "select format_number(cast(stringIdentity(critic_rating) as double), 4) from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the right argument") {
        testQuery(
          "select format_number(critic_rating, cast(stringIdentity(4) as int)) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("StringRepeat") {
      it("works") {
        testQuery("select id, repeat(critic_review, floor(critic_rating)) as x from movies")
      }
      it("works with empty string") {
        testQuery("select id, repeat('', floor(critic_rating)) as x from movies")
      }
      it("works with negative times") {
        testQuery(
          """
            |select
            | id,
            | repeat(critic_review, -floor(critic_rating)) as x1,
            | -floor(critic_rating) as x2
            |from movies
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("udf in the left argument") {
        testQuery(
          "select id, repeat(stringIdentity(critic_review), -floor(critic_rating)) as x from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the right argument") {
        testQuery(
          "select id, repeat(critic_review, -stringIdentity(floor(critic_rating))) as x from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("StringReplace") {
      it("works") {
        testQuery("select id, replace(critic_review, 'an', 'AAA') from movies")
      }
      it("works when second argument is empty") {
        testQuery("select id, replace(critic_review, '', 'A') from movies")
      }
      it("works when third argument is empty") {
        testQuery("select id, replace(critic_review, 'a', '') from movies")
      }
      it("works with two arguments") {
        testQuery("select id, replace(critic_review, 'a') from movies")
      }
      it("works when all arguments are not literals") {
        testQuery("select id, replace(critic_review, title, genre) from movies")
      }
      it("udf in the first argument") {
        testQuery(
          "select id, replace(stringIdentity(critic_review), 'an', 'AAA') from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the second argument") {
        testQuery(
          "select id, replace(critic_review, stringIdentity('an'), 'AAA') from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the third argument") {
        testQuery(
          "select id, replace(critic_review, 'an', stringIdentity('AAA')) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("StringOverlay") {
      it("works") {
        testQuery("select id, overlay(email placing '#Glory_To_Ukraine#' from 9) from users")
      }
      it("works with non-empty len") {
        testQuery("select id, overlay(email placing '#Ukraine#' from 9 for 3) from users")
      }
      it("works with comma separated arguments") {
        testQuery("select id, overlay(email, '#Glory_To_Ukraine#', 9, 6) from users")
      }
      it("works when second argument is not literal") {
        testQuery("select id, overlay(email, last_name, 3, 4) from users")
      }
      it("works with negative len") {
        testQuery("select id, overlay(email placing '#Glory_To_Ukraine#' from 9 for -3) from users")
      }
      it("works with negative position") {
        testQuery("select id, overlay(email placing first_name from -2 for 0) from users_sample")
      }
      it("works with len is zero") {
        testQuery("select id, overlay(email, '#Heroyam_Slava#', 3, 0) from users")
      }
      it("works with len is negative") {
        testQuery("select id, overlay(email, '#SLAVA_ZSY#', 3, -4) from users")
      }
      it("works with len is a string") {
        testQuery("select id, overlay(email, '#SLAVA_ZSY#', 3, '2') from users")
      }
      it("works with len is greater than len of replacing string") {
        testQuery("select id, overlay(email placing last_name from 3 for 10) from users")
      }
      it("works with pos is greater than input string string") {
        testQuery("select id, overlay(email, '#Slava_Ukraini#', 70, 3) from users")
      }
      it("works when pos is not literal") {
        testQuery("select id, overlay(email, '#PTHPNH#', age) from users")
      }
      it("works when len is not literal") {
        testQuery("select id, overlay(email, '#Heroyam_Slava#', 3, id) from users")
      }
      it("udf in the first argument") {
        testQuery(
          "select id, overlay(stringIdentity(email), '#Heroyam_Slava#', 3, 4) from users",
          expectPartialPushdown = true
        )
      }
      it("udf in the 'pos' argument") {
        testQuery(
          "select id, overlay(email, '#StandWithUkraine#', stringIdentity(age), 4) from users",
          expectPartialPushdown = true
        )
      }
      it("udf in the 'len' argument") {
        testQuery(
          "select id, overlay(email, '#ZePresident#', 3, stringIdentity(id)) from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("SubstringIndex") {
      it("works with negative count") {
        testQuery("select id, substring_index(critic_review, ' ', -100) from movies")
      }
      it("works with zero count") {
        testQuery("select id, substring_index(critic_review, ' ', 0) from movies")
      }
      it("works with small count") {
        testQuery("select id, substring_index(critic_review, ' ', 2) from movies")
      }
      it("works with large count") {
        testQuery("select id, substring_index(critic_review, ' ', 100) from movies")
      }
      it("works when delimiter and count are not literals") {
        testQuery("select id, substring_index(critic_review, title, id) from movies")
      }
      it("udf in the first argument") {
        testQuery(
          "select id, substring_index(stringIdentity(critic_review), 'an', '2') from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the second argument") {
        testQuery(
          "select id, substring_index(critic_review, stringIdentity(' '), '2') from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the third argument") {
        testQuery(
          "select id, substring_index(critic_review, ' ', stringIdentity(2)) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("StringLocate") {
      it("works with negative start") {
        testQuery("select id, locate(critic_review, ' ', -100) from movies")
      }
      it("works with zero start") {
        testQuery("select id, locate(critic_review, ' ', 0) from movies")
      }
      it("works with small start") {
        testQuery("select id, locate(critic_review, ' ', 2) from movies")
      }
      it("works with large start") {
        testQuery("select id, locate(critic_review, ' ', 100) from movies")
      }
      it("works when str and start are not literals") {
        testQuery("select id, locate(critic_review, title, id) from movies")
      }
      it("udf in the first argument") {
        testQuery(
          "select id, locate(stringIdentity(critic_review), 'an', '2') from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the second argument") {
        testQuery(
          "select id, locate(critic_review, stringIdentity(' '), '2') from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the third argument") {
        testQuery(
          "select id, locate(critic_review, ' ', stringIdentity(2)) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("StringLPad") {
      it("works with negative len") {
        testQuery("select id, lpad(critic_review, -100, 'ab') from movies")
      }
      it("works with zero len") {
        testQuery("select id, lpad(critic_review, 0, 'ab') from movies")
      }
      it("works with small len") {
        testQuery("select id, lpad(critic_review, 3, 'ab') from movies")
      }
      it("works with large len") {
        testQuery("select id, lpad(critic_review, 1000, 'ab') from movies")
      }
      it("works when len and pad are not literals") {
        testQuery("select id, lpad(critic_review, id, title) from movies")
      }
      it("udf in the first argument") {
        testQuery(
          "select id, lpad(stringIdentity(critic_review), 2, 'an') from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the second argument") {
        testQuery(
          "select id, lpad(critic_review, stringIdentity(2), ' ') from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the third argument") {
        testQuery(
          "select id, lpad(critic_review, 2, stringIdentity(' ')) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("StringRPad") {
      it("works with negative len") {
        testQuery("select id, rpad(critic_review, -100, 'ab') from movies")
      }
      it("works with zero len") {
        testQuery("select id, rpad(critic_review, 0, 'ab') from movies")
      }
      it("works with small len") {
        testQuery("select id, rpad(critic_review, 3, 'ab') from movies")
      }
      it("works with large len") {
        testQuery("select id, rpad(critic_review, 1000, 'ab') from movies")
      }
      it("works when len and pad are not literals") {
        testQuery("select id, rpad(critic_review, id, title) from movies")
      }
      it("udf in the first argument") {
        testQuery(
          "select id, rpad(stringIdentity(critic_review), 2, 'an') from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the second argument") {
        testQuery(
          "select id, rpad(critic_review, stringIdentity(2), ' ') from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the third argument") {
        testQuery(
          "select id, rpad(critic_review, 2, stringIdentity(' ')) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("Substring") {
      it("works") {
        testQuery("select id, substring(critic_review, 1, 20) from movies")
      }
      it("works when pos is negative") {
        testQuery("select id, substring(critic_review, -1, 20) from movies")
      }
      it("works with empty len") {
        testQuery("select id, substring(critic_review, 5) from movies")
      }
      it("works with negative len") {
        testQuery("select id, substring(critic_review, 5, -4) from movies")
      }
      it("works with non-literals") {
        testQuery("select id, substring(critic_review, id, id) from movies")
      }
      it("udf in the first argument") {
        testQuery(
          "select id, substring(stringIdentity(critic_review), 5, 4) from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the second argument") {
        testQuery(
          "select id, substring(critic_review, stringIdentity(5), 4) from movies",
          expectPartialPushdown = true
        )
      }
      it("udf in the third argument") {
        testQuery(
          "select id, substring(critic_review, 5, stringIdentity(4)) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("Initcap") {
      it("works") {
        // Here we don't expect same result as spark implementation of `initcap` method
        // does not define as the beginning of a word that is enclosed in quotation marks / brackets, etc.
        testQuery("select id, initcap(critic_review) from movies", expectSameResult = false)
      }
      it("same result on simple text") {
        testQuery("select id, initcap(favorite_color) from users")
      }
      it("works with ints") {
        testQuery("select id, initcap(id) from movies")
      }
      it("partial pushdown whith udf") {
        testQuery(
          "select id, initcap(stringIdentity(critic_review)) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("StringTranslate") {
      it("works") {
        testQuery("select id, translate(email, 'com', '123') from users")
      }
      it("works when 'from' argument is longer than 'to'") {
        testQuery("select id, translate(email, 'coma', '123') from users")
      }
      it("works when 'to' argument is longer than 'from'") {
        testQuery("select id, translate(email, 'com', '1234') from users")
      }
      it("works when 'from' argument is empty") {
        testQuery("select id, translate(email, '', '123') from users")
      }
      it("works when 'to' argument is empty") {
        testQuery("select id, translate(email, 'abb', '') from users")
      }
      it("works when both 'from' and 'to' arguments are empty") {
        testQuery("select id, translate(email, '', '') from users")
      }
      it("partial pushdown when the second argument is not literal") {
        testQuery(
          "select id, translate(email, last_name, '1234') from users",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown when the third argument is not literal") {
        testQuery(
          "select id, translate(email, 'com', last_name) from users",
          expectPartialPushdown = true
        )
      }
      it("udf in the first argument") {
        testQuery(
          "select id, translate(stringIdentity(email), '@', '#') from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("Upper") {
      it("works") {
        testQuery("select id, upper(critic_review) from movies")
      }
      it("works with ints") {
        testQuery("select id, upper(id) from movies")
      }
      it("partial pushdown whith udf") {
        testQuery(
          "select id, upper(stringIdentity(critic_review)) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("Lower") {
      it("works") {
        testQuery("select id, lower(critic_review) from movies")
      }
      it("works with ints") {
        testQuery("select id, lower(id) from movies")
      }
      it("partial pushdown with udf") {
        testQuery(
          "select id, lower(stringIdentity(critic_review)) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("Left") {
      it("works") {
        testQuery("select id, left(last_name, 2) as l from users_sample")
      }
      it("works with len is sting ") {
        testQuery("select id, left(last_name, '4') as l from users_sample")
      }
      it("partial pushdown with udf") {
        testQuery(
          "select id, left(stringIdentity(critic_review), 4) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("Right") {
      it("works") {
        testQuery("select id, right(critic_review, 2) as r from movies")
      }
      it("works with len is sting") {
        testQuery("select id, right(critic_review, '4') as r from movies")
      }
      it("partial pushdown whith udf") {
        testQuery(
          "select id, right(stringIdentity(critic_review), 4) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("Concat_ws") {
      it("works") {
        testQuery("select id, CONCAT_WS('@', id, 'user.com') as conc from movies")
      }
      it("works with many expressions") {
        testQuery(
          "select id, CONCAT_WS('@', last_name, 'singlestore', '.', 'com', id) as conc from users"
        )
      }
      it("partial pushdown whith udf") {
        testQuery(
          "select id, CONCAT_WS('@', stringIdentity(critic_review), 'user.com') as conc from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("StringSpace") {
      it("works") {
        testQuery("select id, space(floor(critic_rating)) as x from movies")
      }
      it("partial pushdown with udf") {
        testQuery("select id, space(stringIdentity(id)) from movies", expectPartialPushdown = true)
      }
    }

    describe("Length") {
      it("works with strings") {
        testQuery("select id, length(critic_review) from movies")
      }
      it("works with binary") {
        testQuery("select id, length(cast(critic_review as binary)) from movies")
      }
      it("partial pushdown with udf") {
        testQuery("select id, length(stringIdentity(id)) from movies", expectPartialPushdown = true)
      }
    }

    describe("BitLength") {
      it("works with strings") {
        testQuery("select id, bit_length(critic_review) from movies")
      }
      it("works with binary") {
        testQuery("select id, bit_length(cast(critic_review as binary)) from movies")
      }
      it("partial pushdown with udf") {
        testQuery(
          "select id, bit_length(stringIdentity(id)) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("OctetLength") {
      it("works with strings") {
        testQuery("select id, octet_length(critic_review) from movies")
      }
      it("works with binary") {
        testQuery("select id, octet_length(cast(critic_review as binary)) from movies")
      }
      it("partial pushdown with udf") {
        testQuery(
          "select id, octet_length(stringIdentity(id)) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("Ascii") {
      it("works") {
        testQuery("select id, ascii(critic_review) from movies")
      }
      it("partial pushdown with udf") {
        testQuery(
          "select id, ascii(stringIdentity(critic_review)) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("Chr") {
      it("works") {
        testQuery("select id, chr(floor(critic_rating)) as ch from movies")
      }
      it("partial pushdown with udf") {
        testQuery("select id, chr(stringIdentity(id)) from movies", expectPartialPushdown = true)
      }
    }

    describe("Base64") {
      // Spark 3.3|3.4|3.5 use RFC 2045 encoding which has the following behavior:
      //  The encoded output must be represented in lines of no more than 76 characters each and
      //  uses a carriage return '\r' followed immediately by a linefeed '\n' as the line separator.
      //
      // For example, in the following, the comparison of the expected vs. the actual result
      // will fail even though the value is the same:
      // [72,IGxhY2luaWEgbmlzaSB2ZW5lbmF0aXMgdHJpc3RpcXVlLiBGdXNjZSBjb25ndWUsIGRpYW0gaWQgb3JuYXJlIGltcGVyZGlldCwgc2FwaWVuIHVybmEgcHJldGl1bSBuaXNsLCB1dCB2b2x1dHBhdCBzYXBpZW4gYXJjdSBzZWQgYXVndWUuIEFsaXF1YW0gZXJhdCB2b2x1dHBhdC4KCkluIGNvbmd1ZS4gRXRpYW0ganVzdG8uIEV0aWFtIHByZXRpdW0gaWFjdWxpcyBqdXN0by4=]
      // [72,IGxhY2luaWEgbmlzaSB2ZW5lbmF0aXMgdHJpc3RpcXVlLiBGdXNjZSBjb25ndWUsIGRpYW0gaWQg
      //     b3JuYXJlIGltcGVyZGlldCwgc2FwaWVuIHVybmEgcHJldGl1bSBuaXNsLCB1dCB2b2x1dHBhdCBz
      //     YXBpZW4gYXJjdSBzZWQgYXVndWUuIEFsaXF1YW0gZXJhdCB2b2x1dHBhdC4KCkluIGNvbmd1ZS4g
      //     RXRpYW0ganVzdG8uIEV0aWFtIHByZXRpdW0gaWFjdWxpcyBqdXN0by4=]
      //
      // We use substr(col, 0, 76) [first76 chars] and substr(col, -1, 76) [last 76 chars] to make
      // sure we test the pushdown functionality, the validity of the results and allow the test
      // to succeed for Spark 3.3 (and the following versions)
      it("works", ExcludeFromSpark34, ExcludeFromSpark35) {
        testQuery(
          """
            |select
            | id,
            | substr(base64(critic_review), 1, 76) as t_col0,
            | substr(base64(critic_review), -1, 76) as t_col1
            |from movies
            |""".stripMargin.linesIterator.mkString(" ")
        )
      }
      it("partial pushdown with udf") {
        testQuery(
          "select id, base64(stringIdentity(critic_review)) from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("UnBase64") {
      it("works", ExcludeFromSpark34, ExcludeFromSpark35) {
        testQuery("select id, unbase64(base64(critic_review)) as x from movies")
      }
      it("partial pushdown with udf") {
        testQuery(
          "select id, unbase64(base64(stringIdentity(critic_review))) from movies",
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("decimalExpressions") {
    val precisions = List(2, 5, Decimal.MAX_LONG_DIGITS - 10)
    val scales     = List(1, 4, Decimal.MAX_LONG_DIGITS - 11)

    it("sum of decimals") {
      // If precision + 10 <= Decimal.MAX_LONG_DIGITS then DecimalAggregates optimizer will add MakeDecimal and UnscaledValue to this query
      for (precision <- precisions;
           // If rating >= 10^(precision - scale) then rating will overflow during the casting
           // JDBC returns null on overflow if !ansiEnabled and errors otherwise
           // SingleStore truncates the value on overflow
           // Because of this, we skip the case when scale is equals to precision (all rating values are less then 10)
           scale <- scales if scale < precision) {
        testSingleReadForOldS2(
          s"select sum(cast(rating as decimal($precision, $scale))) as rs from reviews",
          SinglestoreVersion(7, 6, 0)
        )
      }

    }
    it("window expression with sum of decimals") {
      // If precision + 10 <= Decimal.MAX_LONG_DIGITS then DecimalAggregates optimizer will add MakeDecimal and UnscaledValue to this query
      for (precision <- precisions;
           // If rating >= 10^(precision - scale) then rating will overflow during the casting
           // JDBC returns null on overflow if !ansiEnabled and errors otherwise
           // SingleStore truncates the value on overflow
           // Because of this, we skip the case when scale is equals to precision (all rating values are less then 10)
           scale <- scales if scale < precision) {
        testSingleReadForReadFromLeaves(
          s"""
            |select
            | sum(cast(rating as decimal($precision, $scale))) over (order by rating) as out
            |from reviews
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
    }
    it("avg of decimals") {
      // If precision + 4 <= MAX_DOUBLE_DIGITS (15) then DecimalAggregates optimizer will add MakeDecimal and UnscaledValue to this query
      for (precision <- precisions;
           // If rating >= 10^(precision - scale) then rating will overflow during the casting
           // JDBC returns null on overflow if !ansiEnabled and errors otherwise
           // SingleStore truncates the value on overflow
           // Because of this, we skip the case when scale is equals to precision (all rating values are less then 10)
           scale <- scales if scale < precision) {
        testSingleReadForOldS2(
          s"select avg(cast(rating as decimal($precision, $scale))) as rs from reviews",
          SinglestoreVersion(7, 6, 0)
        )
      }
    }
    it("window expression with avg of decimals") {
      // If precision + 4 <= MAX_DOUBLE_DIGITS (15) then DecimalAggregates optimizer will add MakeDecimal and UnscaledValue to this query
      for (precision <- precisions;
           // If rating >= 10^(precision - scale) then rating will overflow during the casting
           // JDBC returns null on overflow if !ansiEnabled and errors otherwise
           // SingleStore truncates the value on overflow
           // Because of this, we skip the case when scale is equals to precision (all rating values are less then 10)
           scale <- scales if scale < precision) {
        testSingleReadForReadFromLeaves(
          s"""
            |select
            | avg(cast(rating as decimal($precision, $scale))) over (order by rating) as out
            |from reviews
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
    }
  }

  describe("SortOrder") {
    it("works asc, nulls first") {
      testOrderedQuery("select * from movies order by critic_rating asc nulls first, id")
    }
    it("works desc, nulls last") {
      testOrderedQuery("select * from movies order by critic_rating desc nulls last, id")
    }
    it("partial pushdown asc, nulls last") {
      testSingleReadQuery(
        "select * from movies order by critic_rating asc nulls last",
        expectPartialPushdown = true
      )
    }
    it("partial pushdown desc, nulls first") {
      testSingleReadQuery(
        "select * from movies order by critic_rating desc nulls first",
        expectPartialPushdown = true
      )
    }
    it("partial pushdown with udf") {
      testSingleReadQuery(
        "select * from movies order by stringIdentity(critic_rating)",
        expectPartialPushdown = true
      )
    }
  }

  describe("Rand") {
    it("integer literal") {
      testQuery("select rand(100)*id from users", expectSameResult = false)
    }
    it("long literal") {
      testQuery("select rand(100L)*id from users", expectSameResult = false)
    }
    it("null literal") {
      testQuery("select rand(null)*id from users", expectSameResult = false)
    }
    it(
      "empty arguments",
      ExcludeFromSpark31,
      ExcludeFromSpark32,
      ExcludeFromSpark33,
      ExcludeFromSpark34,
      ExcludeFromSpark35
    ) {
      // TODO PLAT-5759
      testQuery(
        "select rand()*id from users",
        expectSameResult = false,
        expectCodegenDeterminism = false
      )
    }
    it("should return the same value for the same input") {
      val df1 = spark.sql("select rand(100)*id from (select id from testdb.users order by id)")
      val df2 = spark.sql("select rand(100)*id from (select id from testdb.users order by id)")
      assertApproximateDataFrameEquality(df1, df2, 0.001, orderedComparison = false)
    }
  }

  describe("regular expressions") {
    describe("like") {
      it("simple") {
        testQuery("select * from users where first_name like 'Di'")
      }
      it("simple, both fields") {
        testQuery("select * from users where first_name like last_name")
      }
      it("character wildcard") {
        testQuery("select * from users where first_name like 'D_'")
      }
      it("string wildcard") {
        testQuery("select * from users where first_name like 'D%'")
      }
      it("dumb true") {
        testQuery("select * from users where 1 like 1")
      }
      it("dumb false") {
        testQuery("select 2 like 1 from users")
      }
      it("dumb true once more") {
        testQuery("select first_name from users where first_name like first_name")
      }
      it("partial pushdown left") {
        testQuery(
          "select * from users where stringIdentity(first_name) like 'Di'",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown right") {
        testQuery(
          "select * from users where first_name like stringIdentity('Di')",
          expectPartialPushdown = true
        )
      }
      it("null") {
        testQuery("select critic_review like null from movies")
      }
      it("with escape char") {
        testQuery(
          """
            |select
            | first_name like last_name escape '^',
            | last_name like first_name escape '^'
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("with escape char equal to '/'") {
        testQuery(
          """
            |select
            | first_name like last_name escape '/',
            | last_name like first_name escape '/'
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
    }

    describe("rlike") {
      it("simple") {
        testQuery("select * from users where first_name rlike 'D.'")
      }
      it("simple, both fields") {
        testQuery("select * from users where first_name rlike last_name")
      }
      it("from beginning") {
        testQuery("select * from users where first_name rlike '^D.'")
      }
      it("dumb true") { testQuery("select * from users where 1 rlike 1") }
      it("dumb false") { testQuery("select 2 rlike 1 from users") }
      it("partial pushdown left") {
        testQuery(
          "select * from users where stringIdentity(first_name) rlike 'D.'",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown right") {
        testQuery(
          "select * from users where first_name rlike stringIdentity('D.')",
          expectPartialPushdown = true
        )
      }
      it("null") { testQuery("select critic_review rlike null from movies") }
    }

    describe("(not) like all/any patterns functions") {
      val functions = Seq("like all", "like any", "not like all", "not like any")

      it("simple") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(s"select id, first_name from users where first_name $f ('An%te%')")
        }
      }
      it("simple, both fields") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(s"select id, first_name from users where first_name $f (last_name, 'Al%')")
        }
      }
      it("repeated pattern") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(
            s"select id, first_name from users where first_name $f ('Al%', last_name, 'Al%')")
        }
      }
      it("character wildcard") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(s"select * from users where first_name $f ('A___e', '_n__e')")
        }
      }
      it("string wildcard") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(s"select * from users where first_name $f ('Kon%ce', '%tan%', '%Kon%tan%ce%')")
        }
      }
      it("dumb true") {
        for (i <- 0 to 1) {
          log.debug(s"testing ${functions(i)}")
          testQuery(s"select * from users where '1' ${functions(i)} ('1')")
        }
      }
      it("dumb false") {
        for (i <- 0 to 1) {
          log.debug(s"testing ${functions(i)}")
          testQuery(
            s"select * from users where id ${functions(i)} ('D%', 'A%bbbb%')",
            expectEmpty = true
          )
        }
        for (i <- 2 to 3) {
          log.debug(s"testing ${functions(i)}")
          testQuery(s"select * from users where id ${functions(i)} ('D%', 'A%bbbb%')")
        }
      }
      it("dumb true once more") {
        for (i <- 0 to 1) {
          log.debug(s"testing ${functions(i)}")
          testQuery(s"select * from users where first_name ${functions(i)} (first_name)")
        }
        for (i <- 2 to 3) {
          log.debug(s"testing ${functions(i)}")
          testQuery(
            s"select * from users where first_name ${functions(i)} (first_name)",
            expectEmpty = true
          )
        }
      }
      it("null") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(s"select critic_review $f (null) from movies")
        }
      }
      it("partial pushdown left") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(
            s"select * from users where stringIdentity(first_name) $f ('Ali%')",
            expectPartialPushdown = true
          )
        }
      }
      it("partial pushdown right") {
        for (f <- functions) {
          log.debug(s"testing $f")
          testQuery(
            s"select * from users where first_name $f (stringIdentity('Ali%'))",
            expectPartialPushdown = true
          )
        }
      }
      it("very simple patterns", ExcludeFromSpark34, ExcludeFromSpark35) {
        for (f <- functions) {
          log.debug(s"testing $f")
          // Spark 3.1|3.2|3.3 compute these in more optimal way and do not invoke pushdown
          testQuery(
            s"select * from users where first_name $f ('A%', '%b%', '%e')",
            expectPartialPushdown = true
          )
        }
      }
      ignore(
        "09/2024 - very simple patterns full pushdown",
        ExcludeFromSpark31,
        ExcludeFromSpark32,
        ExcludeFromSpark33
      ) {
        for (f <- functions) {
          log.debug(s"testing $f")
          // Spark 3.4|3.5 invoke full pushdown
          testQuery(s"select * from users where first_name $f ('A%', '%b%', '%e')")
        }
      }
      it("empty patterns arg") {
        for (f <- functions) {
          log.debug(s"testing $f")
          try {
            testQuery(s"select * from users where first_name $f ()", expectPartialPushdown = true)
          } catch {
            case e: Throwable =>
              if (e.toString.contains("Expected something between '(' and ')'")) {
                None
              } else { throw e }
          }
        }
      }
    }

    describe("regexp") {
      it("simple") {
        testQuery("select * from users where first_name regexp 'D.'")
      }
      it("simple, both fields") {
        testQuery("select * from users where first_name regexp last_name")
      }
      it("dumb true") { testQuery("select * from users where 1 regexp 1") }
      it("dumb false") { testQuery("select 2 regexp 1 from users") }
      it("partial pushdown left") {
        testQuery(
          "select * from users where stringIdentity(first_name) regexp 'D.'",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown right") {
        testQuery(
          "select * from users where first_name regexp stringIdentity('D.')",
          expectPartialPushdown = true
        )
      }
      it("null") { testQuery("select critic_review regexp null from movies") }
    }

    describe("regexpReplace") {
      it("simple") {
        testQuery("select regexp_replace(first_name, 'D', 'd') from users")
      }
      it("works correctly") {
        testQuery("select * from users where regexp_replace(first_name, 'D', 'd') = 'di'")
      }
      it("partial pushdown") {
        testQuery(
          "select regexp_replace(stringIdentity(first_name), 'D', 'd') from users",
          expectPartialPushdown = true
        )
      }
      it("null") {
        testQuery("select regexp_replace(title, 'D', critic_review) from movies")
      }
      it("non-literals") {
        testQuery("select regexp_replace(first_name, first_name, first_name) from users")
      }
      it("with position") {
        testQuery("select regexp_replace(first_name, 'a', 'd', 3) from users")
      }
      it("big position") {
        testQuery("select regexp_replace(first_name, 'a', 'd', 100) from users")
      }
    }
  }

  describe("automaticLite parallel read") {
    it("no sort in the query") { testQuery("select id from users") }
    it("non top-level sort") {
      testQuery(
        "select id from (select id from users order by id) group by id",
        expectSingleRead = !canDoParallelReadFromAggregators
      )
    }
    it("top-level sort") {
      testQuery(
        "select id from users order by id",
        expectSingleRead = true,
        alreadyOrdered = true
      )
    }
  }

  describe("json functions") {
    describe("GetJsonObject") {
      it("works with strings") {
        testQuery(
          "select id, get_json_object(movie_rating, '$.title') as get_j from movies_rating"
        )
      }
      it("works with int") {
        testQuery(
          "select id, get_json_object(movie_rating, '$.movie_id') as get_j from movies_rating"
        )
      }
      it("works with booleans") {
        testQuery("select id, get_json_object(movie_rating, '$.3D') as get_j from movies_rating")
      }
      it("works with arrays") {
        testQuery(
          "select id, get_json_object(movie_rating, '$.reviews') as get_j from movies_rating"
        )
      }
      it("works with nested json") {
        testQuery(
          "select id, get_json_object(movie_rating, '$.timetable.hall') as get_j from movies_rating"
        )
      }
      it("works with nested json string") {
        testQuery(
          "select id, get_json_object(movie_rating, '$.timetable.day') as get_j from movies_rating"
        )
      }
      it("non-existing  path") {
        testQuery(
          "select id, get_json_object(movie_rating, '$.nonexistingPath') as get_j from movies_rating"
        )
      }
      it("invalid path") {
        testQuery(
          "select id, get_json_object(movie_rating, 'rating') as get_j from movies_rating",
          expectPartialPushdown = true
        )
      }
      it("udf in the first argument") {
        testQuery(
          """
            |select
            | id,
            | get_json_object(stringIdentity(movie_rating), '$.title') as get_j
            |from movies_rating
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
      it("udf in the second argument") {
        testQuery(
          """
            |select
            | id,
            | get_json_object(movie_rating, stringIdentity('$.title')) as get_j
            |from movies_rating
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    describe("LengthOfJsonArray") {
      it("works") {
        testQuery("select id, json_array_length(same_rate_movies) from movies_rating")
      }
      it("udf") {
        testQuery(
          "select id, json_array_length(stringIdentity(same_rate_movies)) from movies_rating",
          expectPartialPushdown = true
        )
      }

    }
  }

  describe("misc functions") {
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

    describe("UUID") {
      it("correct query") {
        if (version.atLeast(SinglestoreVersion(7, 5, 0))) {
          testQuery("select uuid(), id from users_sample", expectSameResult = false)
          testUUIDPushdown("select uuid(), id from users_sample")
        } else {
          testQuery(
            "select uuid(), id from users_sample",
            expectSameResult = false,
            expectPartialPushdown = true
          )
        }
      }
      it("partial pushdown with udf") {
        testQuery(
          "select uuid(), stringIdentity(first_name) from users",
          expectPartialPushdown = true,
          expectSameResult = false
        )
        testUUIDPushdown("select uuid(), stringIdentity(first_name) from users")
      }
    }
  }
}
