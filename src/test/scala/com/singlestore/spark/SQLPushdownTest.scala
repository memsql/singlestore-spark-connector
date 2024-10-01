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
    spark.udf.register("longIdentity", (x: Long) => x)
    spark.udf.register("integerIdentity", (x: Int) => x)
    spark.udf.register("integerFilter", (x: Int) => x % 3 == 1)
    spark.udf.register("floatIdentity", (x: Float) => x)
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
      "all generated SingleStore queries should be the same"
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
      assert(
        singlestoreDF.rdd.getNumPartitions == 1,
        "query is expected to read from a single partition"
      )
    } else {
      assert(
        singlestoreDF.rdd.getNumPartitions > 1,
        "query is expected to read from multiple partitions"
      )
    }

    assert(
      (singlestoreDF.queryExecution.optimizedPlan match {
        case SQLGen.Relation(_) => false
        case _                  => true
      }) == expectPartialPushdown,
      s"the Optimized Plan does not match expectPartialPushdown=$expectPartialPushdown"
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

  def testSingleReadForOldS2(
    q: String,
    minVersion: SinglestoreVersion,
    expectPartialPushdown: Boolean = false,
    expectSingleRead: Boolean = false
  ): Unit = {
    if (version.atLeast(minVersion) && canDoParallelReadFromAggregators) {
      testQuery(
        q,
        expectPartialPushdown = expectPartialPushdown,
        expectSingleRead = expectSingleRead
      )
    } else {
      testSingleReadQuery(q, expectPartialPushdown = expectPartialPushdown)
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

  describe("sanity test the tables") {
    val tables = Seq("users", "users_sample", "movies", "reviews").sorted
    for (t <- tables) { it(s"select all $t") { testQuery(s"select * from $t") } }
  }

  describe("sanity test the `automaticLite` parallel read") {
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

  describe("sanity test the `disablePushdown` flag") {
    val tables = Seq("users", "users_sample", "movies", "reviews").sorted

    for (t <- tables) { it(s"select all $t") { testNoPushdownQuery(s"select * from $t") } }

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

  describe("sanity test partial pushdown") {
    it("ignores spark UDFs") {
      testQuery(
        "select stringUpper(first_name), id from users where id in (10,11,12)",
        expectPartialPushdown = true
      )
    }
    it("join with pure jdbc relation when spark pushdown is not supported") {
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

  describe("DataTypes") {
    // due to a bug in our dataframe comparison library we need to alias the column 4.9 to x...
    // this is because when the library asks spark for a column called "4.9", spark thinks the
    // library wants the table 4 and column 9.
    it("float literal") { testQuery("select 4.9 as t_col from movies") }

    it("negative float literal") { testQuery("select -24.345 as t_col from movies") }
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
        "select typeof(user_id), typeof(created), typeof(review) from reviews"
      )
    }
  }

  describe("Filter") {
    it("numeric equality") { testQuery("select * from users where id = 1") }
    it("numeric inequality") { testQuery("select * from users where id != 1") }
    it("numeric comparison >") { testQuery("select * from users where id > 500") }
    it("numeric comparison > <") { testQuery("select * from users where id > 500 and id < 550") }
    it("string equality") { testQuery("select * from users where first_name = 'Evan'") }
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
          "select longIdentity(id) as user_id from users",
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
    val f = "cast"

    describe(s"$f to boolean") {
      it("boolean") { testQuery(s"select $f(owns_house as boolean) as $f from users") }
      it("int") { testQuery(s"select $f(age as boolean) as $f from users") }
      it("long") { testQuery(s"select $f(id as boolean) as $f from users") }
      it("float") { testQuery(s"select $f(critic_rating as boolean) as $f from movies") }
      it("date") { testQuery(s"select $f(birthday as boolean) as $f from users") }
      it("timestamp") { testQuery(s"select $f(created as boolean) as $f from reviews") }
    }

    describe(s"$f to byte") {
      it("boolean") { testQuery(s"select $f(owns_house as byte) as $f from users") }
      it("int") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min TINYINT value spark returns module (452->-60)
        testQuery(s"select $f(age as byte) as $f from users where age > -129 and age < 128")
      }
      it("long") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min TINYINT value spark returns module (452->-60)
        testQuery(s"select $f(id as byte) as $f from users where id > -129 and id < 128")
      }
      it("float") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min TINYINT value spark returns module (452->-60)
        // singlestore and spark use different rounding
        // singlestore round to the closest value spark round to the smaller one
        testQuery(
          s"""
            |select $f(critic_rating as byte) as $f
            |from movies
            |where critic_rating > -129 and critic_rating < 128 and critic_rating - floor(critic_rating) < 0.5
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("date") { testQuery(s"select $f(birthday as byte) as $f from users") }
      it("timestamp") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min TINYINT value spark returns module (452->-60)
        testQuery(
          s"""
            |select $f(created as byte) as $f
            |from reviews
            |where cast(created as long) > -129 and cast(created as long) < 128
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
    }

    describe(s"$f to short") {
      it("boolean") { testQuery(s"select $f(owns_house as short) as $f from users") }
      it("int") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min SMALLINT value spark returns module (40004->-25532)
        testQuery(s"select $f(age as short) as $f from users where age > -32769 and age < 32768")
      }
      it("long") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min SMALLINT value spark returns module (40004->-25532)
        testQuery(s"select $f(id as short) as $f from users where id > -32769 and id < 32768")
      }
      it("float") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min SMALLINT value spark returns module (40004->-25532)
        // singlestore and spark use different rounding
        // singlestore round to the closest value spark round to the smaller one
        testQuery(
          s"""
            |select $f(critic_rating as short) as $f
            |from movies
            |where critic_rating > -32769 and critic_rating < 32768 and critic_rating - floor(critic_rating) < 0.5
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("date") { testQuery(s"select $f(birthday as short) as $f from users") }
      it("timestamp") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min SMALLINT value spark returns module (40004->-25532)
        testQuery(
          s"""
            |select $f(created as short) as $f
            |from reviews
            |where cast(created as long) > -32769 and cast(created as long) < 32768
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
    }

    describe(s"$f to int") {
      it("boolean") { testQuery(s"select $f(owns_house as int) as $f from users") }
      it("int") { testQuery(s"select $f(age as int) as $f from users") }
      it("long") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min INT value spark returns module (10000000000->1410065408)
        testQuery(
          s"select $f(id as int) as $f from users where id > -2147483649 and id < 2147483648"
        )
      }
      it("float") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min INT value spark returns module (10000000000->1410065408)
        // singlestore and spark use different rounding
        // singlestore round to the closest value spark round to the smaller one
        testQuery(
          s"""
            |select $f(critic_rating as int) as $f
            |from movies
            |where critic_rating > -2147483649 and critic_rating < 2147483648 and critic_rating - floor(critic_rating) < 0.5
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("date") { testQuery(s"select $f(birthday as int) as $f from users") }
      it("timestamp") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min INT value spark returns module (10000000000->1410065408)
        testQuery(
          s"""
            |select $f(created as int) as $f
            |from reviews
            |where cast(created as long) > -2147483649 and cast(created as long) < 2147483648
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
    }

    describe(s"$f to long") {
      it("boolean") { testQuery(s"select $f(owns_house as long) as $f from users") }
      it("int") { testQuery(s"select $f(age as long) as $f from users") }
      it("long") { testQuery(s"select $f(id as long) as $f from users") }
      it("float") {
        // singlestore and spark use different rounding
        // singlestore round to the closest value spark round to the smaller one
        testQuery(
          s"""
            |select $f(critic_rating as long) as $f
            |from movies
            |where critic_rating - floor(critic_rating) < 0.5
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("date") { testQuery(s"select $f(birthday as long) as $f from users") }
      it("timestamp") { testQuery(s"select $f(created as long) as $f from reviews") }
    }

    describe(s"$f to float") {
      it("boolean") { testQuery(s"select $f(owns_house as float) as $f from users") }
      it("int") { testQuery(s"select $f(age as float) as $f from users") }
      it("long") { testQuery(s"select $f(id as float) as $f from users") }
      it("float") { testQuery(s"select $f(critic_rating as float) as $f from movies") }
      it("date") { testQuery(s"select $f(birthday as float) as $f from users") }
      it("timestamp") { testQuery(s"select $f(created as float) as $f from reviews") }
    }

    describe(s"$f to double") {
      it("boolean") { testQuery(s"select $f(owns_house as double) as $f from users") }
      it("int") { testQuery(s"select $f(age as double) as $f from users") }
      it("long") { testQuery(s"select $f(id as double) as $f from users") }
      it("float") { testQuery(s"select $f(critic_rating as double) as $f from movies") }
      it("date") { testQuery(s"select $f(birthday as double) as $f from users") }
      it("timestamp") { testQuery(s"select $f(created as double) as $f from reviews") }
    }

    describe(s"$f to decimal") {
      it("boolean") { testQuery(s"select $f(owns_house as decimal(20, 5)) as $f from users") }
      it("int") { testQuery(s"select $f(age as decimal(20, 5)) as $f from users") }
      it("long") {
        // singlestore and spark behaviour differs on the overflow
        // singlestore returns max/min DECIMAL(x, y) value spark returns null
        testQuery(
          s"select $f(id as decimal(20, 5)) as $f from users where id < 999999999999999.99999"
        )
      }
      it("float") { testQuery(s"select $f(critic_rating as decimal(20, 5)) as $f from movies") }
      it("date") { testQuery(s"select $f(birthday as decimal(20, 5)) as $f from users") }
      it("timestamp") { testQuery(s"select $f(created as decimal(20, 5)) as $f from reviews") }
    }

    describe(s"$f to string") {
      it("boolean") { testQuery(s"select $f(owns_house as string) as $f from users") }
      it("int") { testQuery(s"select $f(age as string) as $f from users") }
      it("long") { testQuery(s"select $f(id as string) as $f from users") }
      it("float") {
        // singlestore converts integers to string with 0 digits
        // after the point spark adds 1 digit after the point
        testQuery(s"select $f($f(critic_rating as string) as float) as $f from movies")
      }
      it("date") { testQuery(s"select $f(birthday as string) as $f from users") }
      it("timestamp") {
        // singlestore converts timestamps to string with 6 digits
        // after the point spark adds 0 digit after the point
        testQuery(s"select $f(cast(created as string) as timestamp) as $f from reviews")
      }
      it("string") { testQuery(s"select $f(first_name as string) as $f from users") }
    }

    describe(s"$f to binary") {
      // spark doesn't support casting other types to binary
      it("string") { testQuery(s"select $f(first_name as binary) as $f from users") }
    }

    describe(s"$f to date") {
      it("date") { testQuery(s"select $f(birthday as date) as $f from users") }
      it("timestamp") { testQuery(s"select $f(created as date) as $f from reviews") }
      it("string") { testQuery(s"select $f(cast(birthday as string) as date) as $f from users") }
    }

    describe(s"$f to timestamp") {
      it("boolean") { testQuery(s"select $f(owns_house as timestamp) as $f from users") }
      it("int") { testQuery(s"select $f(age as timestamp) as $f from users") }
      it("long") {
        // TIMESTAMP in SingleStore doesn't support values greater then 2147483647999
        testQuery(s"select $f(id as timestamp) as $f from users where id < 2147483647999")
      }
      it("float") {
        // singlestore and spark use different rounding
        // singlestore round to the closest value spark round to the smaller one
        testQuery(
          s"""
            |select to_unix_timestamp($f(critic_rating as timestamp)) as $f
            |from movies
            |where critic_rating - floor(critic_rating) < 0.5
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it("date") { testQuery(s"select $f(birthday as timestamp) as $f from users") }
      it("timestamp") { testQuery(s"select $f(created as timestamp) as $f from reviews") }
      it("string") {
        testQuery(s"select $f($f(birthday as string) as timestamp) as $f from users")
      }
    }
  }

  describe("Variable Expressions") {
    describe("Coalesce") {
      val f = "coalesce"

      it(s"$f one non-null value") { testQuery(s"select $f(id) as $f from users") }
      it(s"$f one null value") {
        testSingleReadForReadFromLeaves(s"select $f(null) as $f from users")
      }
      it(s"$f a lot of values") {
        testQuery(s"select $f(null, id, null, id + 1) as $f from users")
      }
      it(s"$f a lot of nulls") {
        testSingleReadForReadFromLeaves(s"select $f(null, null, null) as $f from users")
      }
      it(s"$f with partial pushdown with udf") {
        testQuery(
          s"select $f(stringIdentity(first_name), 'qwerty', 'bob', 'alice') as $f from users",
          expectPartialPushdown = true
        )
      }
    }

    val functions = Seq("least", "greatest", "concat", "elt").sorted

    for (f <- functions) {
      describe(f.capitalize) {
        it(s"$f a lot of ints") { testQuery(s"select $f(id+5, id, 5, id+1) as $f from users") }
        it(s"$f a lot of strings") {
          testQuery(s"select $f('qwerty', 'bob', first_name, 'alice') as $f from users")
        }

        if (Seq("concat", "elt").contains(f)) {
          it(s"$f ints with null") {
            testQuery(s"select $f(null, id, null, id+1) as $f from users")
          }
        } else {
          // singlestore returns NULL if at least one argument is NULL, when spark skips nulls
          ignore(s"09/2024 - $f ints with null") {
            testQuery(s"select $f(null, id, null, id+1) as $f from users")
          }
        }

        it(s"$f a lot of nulls") {
          if (Seq("elt").contains(f)) {
            testQuery(s"select $f(null, null, null) as $f from users")
          } else {
            testSingleReadForReadFromLeaves(s"select $f(null, null, null) as $f from users")
          }
        }

        if (Seq("concat", "elt").contains(f)) {
          it(s"$f int and string") { testQuery(s"select $f(id, first_name) as $f from users") }
        }

        if (Seq("concat").contains(f)) {
          it(s"$f same expressions") { testQuery(s"select $f(id, id, id) as $f from users") }
          it(s"$f nested same expressions") {
            testQuery(s"select * from (select $f(id, id, id) as $f from users)")
          }
        }

        it(s"$f with partial pushdown with udf") {
          testQuery(
            s"select $f('qwerty', 'bob', stringIdentity(first_name), 'alice') as $f from users",
            expectPartialPushdown = true
          )
        }
      }
    }
  }

  describe("Null Expressions") {
    describe("IfNull") {
      val f = "IfNull"

      it(s"$f returns the second argument") {
        testQuery(s"select $f(null, id) as ${f.toLowerCase} from users")
      }
      it(s"$f returns the first argument") {
        testQuery(s"select $f(id, null) as ${f.toLowerCase} from users")
      }
      it(s"$f with partial pushdown with udf") {
        testQuery(
          s"select $f(longIdentity(id), id) as ${f.toLowerCase} from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("NullIf") {
      val f = "nullif"

      it(s"$f equal arguments") { testQuery(s"select id, $f(1, 1) as $f from users") }
      it(s"$f non-equal arguments with exp2 null") {
        testQuery(s"select $f(id, null) as $f from users")
      }
      it(s"$f non-equal arguments") {
        testQuery(s"select $f(id, favorite_color) as $f from users")
      }
      it(s"$f with partial pushdown with udf in exp1") {
        testQuery(
          s"select $f(longIdentity(id), null) as $f from users",
          expectPartialPushdown = true
        )
      }
      // when exp1 is a literal null spark optimizes to null since it's the only possible result,
      // we are using a nullable column in exp1 to further validate the partial pushdown behavior
      // and results
      it(s"$f with partial pushdown with udf in exp2") {
        testQuery(
          s"select $f(favorite_color, stringIdentity(id)) as $f from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("Nvl") {
      val f = "nvl"

      it(s"$f returns the second argument") { testQuery(s"select $f(null, id) as $f from users") }
      it(s"$f returns the first argument") { testQuery(s"select $f(id, id+1) as $f from users") }
      it(s"$f with partial pushdown with udf") {
        testQuery(
          s"select $f(longIdentity(id), null) as $f from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("Nvl2") {
      val f = "Nvl2"

      it(s"$f returns the second argument") {
        testSingleReadForReadFromLeaves(s"select $f(null, id, 0) as ${f.toLowerCase} from users")
      }
      it(s"$f returns the first argument") {
        testQuery(s"select $f(id, 10, id+1) as ${f.toLowerCase} from users")
      }
      it(s"$f with partial pushdown with udf") {
        testQuery(
          s"select $f(stringIdentity(id), null, id) as ${f.toLowerCase} from users",
          expectPartialPushdown = true
        )
      }
    }

    val functions = Seq("IsNull", "IsNotNull").sorted

    for (f <- functions) {
      describe(f) {
        it(s"${f.toLowerCase} returns correct results for nullable column") {
          testQuery(s"select ${f.toLowerCase}(favorite_color) as ${f.toLowerCase} from users")
        }
        it(s"${f.toLowerCase} returns correct result for null") {
          testQuery(s"select ${f.toLowerCase}(null) as ${f.toLowerCase} from users")
        }
        it(s"${f.toLowerCase} with partial pushdown with udf") {
          testQuery(
            s"select ${f.toLowerCase}(stringIdentity(id)) as ${f.toLowerCase} from users",
            expectPartialPushdown = true
          )
        }
      }
    }
  }

  describe("Predicates") {
    describe("Not") {
      val f = "not"

      it(s"$f correctly inverses non-nullable column") {
        testQuery(s"select $f(cast(owns_house as boolean)) as $f from users")
      }
      it(s"$f correctly inverses literal null") { testQuery(s"select $f(null) as $f from users") }

      it(s"$f works with tinyint") { testQuery(s"select $f(owns_house = 1) as $f from users") }
      it(s"$f works with single brackets") { testQuery(s"select $f(id = '10') as $f from users") }
      it(s"$f works with text") { testQuery(s"select $f(first_name = 'Wylie') as $f from users") }
      it(s"$f works with boolean") {
        testQuery(s"select $f(cast(owns_house as boolean) = true) as $f from users")
      }
      it(s"$f works with tinyint not null") {
        testQuery(s"select $f(owns_house = null) as $f from users")
      }
      it(s"$f works with not null") { testQuery(s"select $f(null = null) as $f from users") }

      it(s"$f with partial pushdown with udf") {
        testQuery(
          s"select $f(cast(longIdentity(id) as boolean)) as $f from users",
          expectPartialPushdown = true
        )
        testQuery(s"select $f(stringIdentity(id) = '10') from users", expectPartialPushdown = true)
      }
    }

    describe("In") {
      val f = "in"

      it(s"$f works with tinyint") { testQuery(s"select owns_house $f(1) as $f from users") }
      it(s"$f works with single brackets") {
        testQuery(s"select id $f('10','11','12') as $f from users")
      }
      it(s"$f works with text") {
        testQuery(s"select first_name $f('Wylie', 'Sukey', 'Sondra') as $f from users")
      }
      it(s"$f works with boolean") {
        testQuery(s"select cast(owns_house as boolean) $f(true) as $f from users")
      }
      it(s"$f works with tinyint in literal null") {
        testQuery(s"select owns_house $f(null) as $f from users")
      }
      it(s"$f works with null in literal null") {
        testQuery(s"select null $f(null) as $f from users")
      }
      it(s"$f with partial pushdown because of udf") {
        testQuery(s"select stringIdentity(id) $f('10') from users", expectPartialPushdown = true)
      }
    }

    val functions = Seq("and", "or").sorted

    for (f <- functions) {
      describe(f.capitalize) {
        it(s"${f.capitalize} works with cast and literal true") {
          testQuery(s"select cast(owns_house as boolean) $f true from users")
        }
        it(s"${f.capitalize}  works with cast and literal false") {
          testQuery(s"select cast(owns_house as boolean) $f false from users")
        }
        it(s"${f.capitalize}  works with cast and non-nullable boolean column") {
          testQuery(s"select cast(id as boolean) $f cast(owns_house as boolean) from users")
        }
        it(s"${f.capitalize}  works with literal true and literal false") {
          testQuery(s"select true $f false from users")
        }
        it(s"${f.capitalize}  works with literal null") {
          testQuery(s"select cast(id as boolean) $f null from users")
        }
        it(s"${f.capitalize}  with partial pushdown because of udf") {
          testQuery(
            s"""
               |select
               | cast(stringIdentity(id) as boolean) $f cast(stringIdentity(owns_house) as boolean)
               |from users
               |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
      }
    }

    describe("If") {
      val f = "if"

      it(s"$f with boolean non-nullable column") {
        testQuery(
          s"select $f(cast(owns_house as boolean), first_name, last_name) as $f from users"
        )
      }
      it(s"$f always true") { testQuery(s"select $f(true, first_name, last_name) as $f from users") }
      it(s"$f with partial pushdown with udf") {
        testQuery(
          s"""
            |select $f(cast(longIdentity(id) as boolean), first_name, last_name) as $f
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    describe("Case When") {
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
          "select case when id < 5 then 1 else longIdentity(id) end as t_col from users",
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("Arithmetic Expressions") {
    val functions = Seq(
      ("add", "+"),
      ("subtract", "-"),
      ("multiply", "*"),
      ("divide", "/"),
      ("remainder", "%")
    )

    for ((f, s) <- functions) {
      describe(f.capitalize) {
        it(s"$f numbers") { testQuery(s"select user_id $s movie_id as $f from reviews") }
        it(s"$f floats") {
          testQuery(
            s"""
              |select
              | rating $s ${if (Seq("+", "-").contains(s)) 1.0 else if ( "%" == s) 4 else 1.3 } as $f
              |from reviews
              |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
          )
        }
        it(s"$f with partial pushdown because of udf in the left argument") {
          testQuery(
            s"select stringIdentity(user_id) $s movie_id as $f from reviews",
            expectPartialPushdown = true
          )
        }
        it(s"$f with partial pushdown because of udf in the right argument") {
          testQuery(
            s"select user_id $s stringIdentity(movie_id) as $f from reviews",
            expectPartialPushdown = true
          )
        }
      }

    }

    describe("Pmod") {
      val f = "pmod"

      it(s"$f numbers") { testQuery(s"select $f(user_id, movie_id) as $f from reviews") }
      it(s"$f floats") { testQuery(s"select $f(rating, 4) as $f from reviews") }
      it(s"$f with partial pushdown because of udf in the left argument") {
        testQuery(
          s"select $f(stringIdentity(user_id), movie_id) as $f from reviews",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the right argument") {
        testQuery(
          s"select $f(user_id, stringIdentity(movie_id)) as $f from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("UnaryMinus") {
      val f = "UnaryMinus"

      it(s"$f numbers") { testQuery(s"select -id as ${f.toLowerCase} from users") }
      it(s"$f floats") { testQuery(s"select -critic_rating as ${f.toLowerCase} from movies") }
      it(s"$f with partial pushdown with udf") {
        testQuery(
          s"select -longIdentity(id) as ${f.toLowerCase} from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("UnaryPositive") {
      val f = "UnaryPositive"

      it(s"$f numbers") { testQuery(s"select +id as ${f.toLowerCase} from users") }
      it(s"$f floats") { testQuery(s"select +critic_rating as ${f.toLowerCase} from movies") }
      it(s"$f with partial pushdown with udf") {
        testQuery(
          s"select +longIdentity(id) as ${f.toLowerCase} from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("Abs") {
      val f = "abs"

      it(s"$f positive numbers") { testQuery(s"select $f(id) as $f from users") }
      it(s"$f negative numbers") { testQuery(s"select $f(-id) as $f from users") }
      it(s"$f positive floats") { testQuery(s"select $f(critic_rating) as $f from movies") }
      it(s"$f negative floats") { testQuery(s"select $f(-critic_rating) as $f from movies") }
      it(s"$f with partial pushdown with udf") {
        testQuery(s"select $f(stringIdentity(id)) as $f from users", expectPartialPushdown = true)
      }
    }
  }

  describe("bitwise Expressions") {
    val functionsGroup1 = Seq(
      ("BitwiseAnd", "&"),
      ("BitwiseOr", "|"),
      ("BitwiseXor", "^")
    )

    for ((f, s) <- functionsGroup1) {
      describe(f) {
        it(s"${f.toLowerCase} succeeds") {
          testQuery(s"select user_id $s movie_id as ${f.toLowerCase} from reviews")
        }
        it(s"${f.toLowerCase} with partial pushdown because of udf in the left argument") {
          testQuery(
            s"""
               |select cast(stringIdentity(user_id) as integer) $s movie_id as ${f.toLowerCase}
               |from reviews
               |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
        it(s"${f.toLowerCase} with partial pushdown because of udf in the right argument") {
          testQuery(
            s"""
               |select user_id $s cast(stringIdentity(movie_id) as integer) as ${f.toLowerCase}
               |from reviews
               |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
      }
    }

    describe("BitwiseGet") {
      it("numbers", ExcludeFromSpark31) {
        testQuery("select bit_get(id, 2) as t_col from users_sample")
      }
      it("negative left argument", ExcludeFromSpark31) {
        try {
          testQuery("select bit_get(id, -2) as t_col from users_sample")
        } catch {
          case e: Throwable =>
            if (e.toString.contains("Invalid bit position: -2 is less than zero")) {
              None
            } else { throw e }
        }
      }
      it("negative right argument", ExcludeFromSpark31) {
        testQuery("select bit_get(-200, 2) as t_col, id from users_sample")
      }
      it("exceeds upper limit left argument", ExcludeFromSpark31) {
        try {
          testQuery("select bit_get(id, 100000) as t_col from users_sample")
        } catch {
          case e: Throwable =>
            if (e.toString.contains("Invalid bit position: 100000 exceeds the bit upper limit")) {
              None
            } else { throw e }
        }
      }
      it("big int right argument", ExcludeFromSpark31) {
        testQuery("select bit_get(1000000000000000, 2) as t_col, id from users_sample")
      }
      it("partial pushdown because of udf in the left argument", ExcludeFromSpark31) {
        testQuery(
          "select bit_get(cast(stringIdentity(movie_id) as integer), 2) as t_col from reviews",
          expectPartialPushdown = true
        )
      }
      it("partial pushdown because of udf in the right argument", ExcludeFromSpark31) {
        testQuery(
          "select bit_get(movie_id, cast(stringIdentity(2) as integer)) as t_col from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("BitwiseCount") {
      it("bit_count non-nullable column") {
        testQuery("select bit_count(user_id) as bit_count from reviews")
      }
      it("partial pushdown because of udf") {
        testQuery(
          "select bit_count(longIdentity(user_id)) as bit_count from reviews",
          expectPartialPushdown = true
        )
      }
      it("bit_count with nullable column") {
        testQuery(
          """
            |select
            | id,
            | critic_rating % 2 == 0 as critic_rating_bool,
            | bit_count(critic_rating % 2 == 0) as bit_count
            |from movies
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
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

    val functionsGroup2 = Seq(
      ("BitAndAgg", "bit_and", "user_id", "reviews"),
      ("BitOrAgg", "bit_or", "age", "users"),
      ("BitXorAgg", "bit_xor", "user_id", "reviews")
    )

    for ((f, n, c, t) <- functionsGroup2) {
      describe(f) {
        it(s"$n non-nullable column") { bitOperationTest(s"select $n($c) as $n from $t") }
        it(s"$n with partial pushdown because of udf") {
          bitOperationTest(
            s"select $n(integerIdentity($c)) as $n from $t",
            expectPartialPushdown = true,
            expectSingleRead = true
          )
        }
        it(s"$n filter") {
          bitOperationTest(s"select $n($c) filter (where $c % 2 = 0) as $n from $t")
        }
        // singlestore returns [bit_and|bit_or|bit_xor](null) = [0] whereas
        // spark returns [bit_and|bit_or|bit_xor](null) = [null]
        ignore(s"09/2024 - $n non-nullable column") {
          bitOperationTest(s"select $n($c) as $n from $t")
        }
        it(s"$n with nullable column") {
          bitOperationTest(s"select $n(cast(rint(critic_rating) % 2 as int)) as $n from movies")
        }
      }
    }
  }

  describe("Math Expressions") {
    val functions = Seq(
      "sinh", "cosh", "tanh", "rint", "asinh", "acosh", "sqrt", "ceil", "cos", "exp", "expm1",
      "floor", "signum", "cot", "degrees", "radians", "bin", "hex", "log2", "log10", "log1p",
      "ln" // (Log)
    ).sorted

    for (f <- functions) {
      describe(f) {
        f match {
          case "bin" | "hex" =>
            it(s"$f works with long column") {
              testQuery(s"select user_id, $f(user_id) as $f from reviews")
            }
          case "unhex" =>
            it(s"$f works with string column") {
              testQuery(s"select review, $f(review) as $f from reviews")
            }
          case _ =>
            it(s"$f works with float column") {
              testQuery(
                s"select cast(rating as decimal(2,1)) as rating, $f(rating) as $f from reviews"
              )
            }
        }

        if (!Seq("acosh", "cot").contains(f)) {
          it(s"$f works with tinyint") {
            testQuery(s"select owns_house, $f(owns_house) as $f from users")
          }
        }

        it(s"$f with partial pushdown because of udf") {
          testQuery(
            s"""
              |select
              |${if (f == "hex") s"user_id, $f(longIdentity(user_id)) as $f" else s"rating, $f(floatIdentity(rating)) as $f"},
              |${if (f == "unhex") s"$f(stringIdentity(review)) as $f" else "stringIdentity(review) as review"}
              |from reviews
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
        it(s"$f works with literal null") { testQuery(s"select $f(null) as $f from reviews") }

        f match {
          case "acosh" | "cot" =>
            // acosh:
            //  singlestore does not support NaN and it returns NULL
            //  instead of NaN that is returned from spark
            //
            //  Example:
            //    SingleStore Row [value, acosh(value)] | Spark Row [value, acosh(value)]
            //    [0.800000011920929,null]              | [0.8,NaN]
            //    [0.6000000238418579,null]             | [0.6,NaN]
            //    [0.699999988079071,null]              | [0.7,NaN]
            //    [0.0,null]                            | [0.0,NaN]
            //    [0.5,null]                            | [0.5,NaN]
            //    [0.5,null]                            | [0.5,NaN]
            //
            // cot:
            //  singlestore also does not support Infinity and it returns
            //  NULL instead of Infinity that is returned from spark
            //
            //  Example:
            //    SingleStore Row [value, acosh(value)] | Spark Row [value, acosh(value)]
            //    [0.0,null]                            | [0.0,Infinity]
            it(s"$f works with nullable column") {
              testQuery(
                s"""
                   |select
                   |  critic_rating,
                   |  $f(cast(rint(critic_rating) as decimal(2,0))) as $f
                   |from movies
                   |where critic_rating > 1.0
                   |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
              )
            }
          case "bin" =>
            it(s"$f works with nullable column") {
              testQuery(s"select $f(cast(rint(critic_rating) as decimal(2,0))) as $f from movies")
            }
          case "hex" | "unhex" =>
            it(s"$f works with nullable column") {
              testQuery(s"select $f(critic_review) as $f from movies")
            }
          case _ =>
            it(s"$f works with nullable column") {
              testQuery(s"select $f(critic_rating) as $f from movies")
            }
        }
      }
    }

    describe("hypot") {
      val f = "hypot"

      it(s"$f works with float column") {
        testQuery(s"select $f(rating, user_id) as $f from reviews")
      }
      it(s"$f works with tinyint") { testQuery(s"select $f(owns_house, id) as $f from users") }
      it(s"$f with partial pushdown because of udf") {
        testQuery(
          s"""
            |select
            | $f(floatIdentity(rating), user_id) as $f,
            | stringIdentity(review) as review
            |from reviews
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
      it(s"$f works with literal null") { testQuery(s"select $f(null, null) as $f from reviews") }
      it(s"$f works with simple nullable column") {
        testQuery(s"select $f(critic_rating, id) as $f from movies")
      }
      it(s"$f works with complex nullable columns") {
        testQuery(
          s"""
            |select
            | id,
            | critic_rating,
            | $f(critic_rating, id) as ${f}1,
            | $f(critic_rating, -id) as ${f}2,
            | $f(-critic_rating, id) as ${f}3,
            | $f(-critic_rating, -id) as ${f}4,
            | $f(id, critic_rating) as ${f}5,
            | $f(id, -critic_rating) as ${f}6,
            | $f(-id, critic_rating) as ${f}7,
            | $f(-id, -critic_rating) as ${f}8
            |from movies
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it(s"$f with partial pushdown because of udf in the left argument") {
        testQuery(
          s"select $f(floatIdentity(critic_rating), id) as $f from movies",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the right argument") {
        testQuery(
          s"select $f(critic_rating, longIdentity(id)) as $f from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("atanh") {
      val f = "atanh"

      it(s"$f works with float nullable column") {
        testQuery(
          s"""
            |select
            | critic_rating,
            | $f(critic_rating) as $f
            |from movies
            |where critic_rating > -1 AND critic_rating < 1
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it(s"$f with partial pushdown because of udf") {
        testQuery(
          s"select rating, $f(stringIdentity(rating)) as $f from reviews",
          expectPartialPushdown = true
        )
      }
      it(s"$f works with literal null") { testQuery(s"select $f(null) as $f from reviews") }
    }

    describe("integralDivide") {
      val (f, s) = ("integralDivide", "div")

      it(s"$f works with bigint column") {
        testQuery(s"select user_id, movie_id, user_id $s movie_id as $f from reviews")
      }
      it(s"$f works with float nullable column") {
        testQuery(
          s"""
            |select
            | critic_rating,
            | cast(critic_rating as decimal(20, 5)) $s cast(critic_rating as decimal(20, 5)) as $f
            |from movies
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it(s"$f works with float nullable column in exp1") {
        testQuery(
          s" select critic_rating, cast(critic_rating as decimal(20, 5)) $s 1.0 as $f from movies"
        )
      }
      it(s"$f works with float nullable column in exp2") {
        testQuery(
          s"select critic_rating, 1.0 $s cast(critic_rating as decimal(20, 5)) as $f from movies"
        )
      }
      it(s"$f works with null") { testQuery(s"select null $s null as $f from reviews") }
      it(s"$f with partial pushdown because of udf") {
        testQuery(
          s"select stringIdentity(null $s null) as $f from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("atan2") {
      val f = "atan2"

      // atan(-e,-1) = -pi | atan(e,-1) = pi, where e is a very small value
      // we are filtering this cases, because the result can differ for
      // singlestore and spark because of precision loss
      it(s"$f works with nullable columns") {
        testQuery(
        s"""
          |select
          | id,
          | critic_rating,
          | $f(critic_rating, id) as a1,
          | $f(critic_rating, -id) as a2,
          | $f(-critic_rating, id) as a3,
          | $f(-critic_rating, -id) as a4,
          | $f(-id, -critic_rating) as a5
          |from movies
          |where abs(critic_rating) > 0.01 or critic_rating is null
          |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it(s"$f works with udf in the left argument") {
        testQuery(
          s"select $f(floatIdentity(critic_rating), id) as $f from movies",
          expectPartialPushdown = true
        )
      }
      it(s"$f works with udf in the right argument") {
        testQuery(
          s"select $f(critic_rating, longIdentity(id)) as $f from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("pow|power") {
      val f = "pow"

      it(s"$f works with nullable column in left argument") {
        testQuery(s"select log($f(critic_rating, id)) as $f from movies")
      }
      it(s"$f works with nullable column in right argument") {
        testQuery(s"select log($f(id, critic_rating)) as $f from movies")
      }
      it(s"$f with partial pushdown because of udf in the left argument") {
        testQuery(
          s"select log($f(longIdentity(id), critic_rating)) as $f from movies",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the right argument") {
        testQuery(
          s"select log($f(id, floatIdentity(critic_rating))) as $f from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("log (Logarithm)") {
      val f = "log"

      it(s"$f works with nullable columns") {
        // spark returns +/-Infinity for log(1, x) singlestore returns NULL for it
        testQuery(
        s"""
          |select
          | id,
          | critic_rating,
          | $f(critic_rating, id) as l1,
          | $f(critic_rating, -id) as l2,
          | $f(-critic_rating, id) as l3,
          | $f(-critic_rating, -id) as l4,
          | $f(id, critic_rating) as l5,
          | $f(id, -critic_rating) as l6,
          | $f(-id, critic_rating) as l7,
          | $f(-id, -critic_rating) as l8
          |from movies
          |where id != 1 and critic_rating != 1
          |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it(s"$f works with one argument") {
        testQuery(s"select $f(critic_rating) as $f from movies")
      }
      it(s"$f with partial pushdown because of udf in the left argument") {
        testQuery(
          s"select $f(longIdentity(id), critic_rating) as $f from movies",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the right argument") {
        testQuery(
          s"select $f(id, floatIdentity(critic_rating)) as $f from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("round") {
      val f = "round"

      // singlestore can round x.5 differently
      it(s"$f works with one argument") {
        testQuery(
        s"""
          |select critic_rating, $f(critic_rating) as ${f}1, $f(-critic_rating) as ${f}2
          |from movies
          |where critic_rating - floor(critic_rating) != 0.5
          |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it(s"$f works with two arguments") {
        testQuery(
        s"""
          |select
          | critic_rating,
          | $f(critic_rating/10.0, 1) as ${f}1,
          | $f(-critic_rating/100.0, 2) as ${f}2
          |from movies
          |where critic_rating - floor(critic_rating) != 0.5
          |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it(s"$f works with negative scale") {
        testQuery(s"select $f(critic_rating, -2) as $f from movies")
      }
      // Note: right argument must be foldable so we can't use udf there
      it(s"$f with partial pushdown because of udf in the left argument") {
        testQuery(
          s"select $f(floatIdentity(critic_rating), 2) as $f from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("width_bucket") {
      val f = "width_bucket"

      it(s"$f works with int, simple case") {
        testQuery(s"select id, $f(id, 10, 90, 10) as $f from movies")
      }
      it(s"$f works with int, negative value") {
        testQuery(s"select $f(-id, -90, -10, 10) as $f from movies")
      }
      it(s"$f works with many small buckets") {
        testQuery(s"select id, $f(id, 10, 90, 1000000000) as $f from movies")
      }
      it(s"$f works with int, max < min") {
        testQuery(s"select id, $f(id, 90, 10, 10) as $f from movies")
      }
      it(s"$f works with num_buckets is float") {
        testQuery(s"select id, $f(id, 90, 10, 6.6) as $f from movies")
      }
      it(s"$f works with num_buckets is non-constant float") {
        testQuery(
          s"""
            |select id, $f(id, 10, 90, id/10 + 1) as $f
            |from movies
            |where id/10 - floor(id/10) < 0.5
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it(s"$f works with float, simple case") {
        testQuery(
          s"select id, $f(critic_rating, 10 + critic_rating, 90, 10) as $f from movies"
        )
      }
      it(s"$f works with string") {
        testQuery(
          s"select id, $f(-critic_rating, '10.2', '0.4', '10') as $f from movies"
        )
      }
      it(s"$f works with all args are not const") {
        testQuery(
          s"select id, $f(critic_rating, id-10, id, id + 200) as $f from movies"
        )
      }
      it(s"$f works with int, max = min") {
        testQuery(s"select id, $f(id, 90, 90, 10) as $f from movies")
      }
      it(s"$f works if num_buckets is negative") {
        testQuery(s"select id, $f(id, 90, 10, -1) as $f from movies")
      }
      it(s"$f works with num_buckets = 0") {
        testQuery(s"select id, $f(id, 90, 10, 0) as $f from movies")
      }
      it(s"$f with partial pushdown because of udf in the first arg") {
        testQuery(
          s"select id, $f(floatIdentity(critic_rating), 10, 100, 200) as $f from movies",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the second arg") {
        testQuery(
          s"select id, $f(id, floatIdentity(critic_rating), 100, 200) as $f from movies",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the third arg") {
        testQuery(
          s"select id, $f(id, 10, floatIdentity(critic_rating), 200) as $f from movies",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the fourth arg") {
        testQuery(
          s"select id, $f(id, 100, 200, floatIdentity(critic_rating)) as $f from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("conv") {
      val f = "conv"
      // singlestore and spark behaviour differs when num contains non alphanumeric characters
      val bases = Seq(2, 5, 23, 36)

      for (fromBase <- bases; toBase <- bases) {
        it(s"$f works with non-nullable columns converting $fromBase -> $toBase base") {
          testQuery(
            s"""
               |select
               |  first_name,
               |  $f(first_name, $fromBase, $toBase) as $f
               |from users
               |where first_name rlike '^[a-zA-Z0-9]*$$'
               |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
          )
        }
      }
      it(s"$f works with numeric") { testQuery(s"select $f(id, 10, 2) as $f from users") }
      it(s"$f with partial pushdown when fromBase out of range [2, 36]") {
        testQuery(
          s"select $f(first_name, 1, 20) as $f from users",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown when toBase out of range [2, 36]") {
        testQuery(
          s"select $f(first_name, 20, 1) as $f from users",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the first argument") {
        testQuery(
          s"select $f(stringIdentity(first_name), 20, 15) as $f from users",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the second argument") {
        testQuery(
          s"select $f(first_name, integerIdentity(20), 15) as $f from users",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the third argument") {
        testQuery(
          s"select $f(first_name, 20, integerIdentity(15)) as $f from users",
          expectPartialPushdown = true
        )
      }
    }

    it("EulerNumber") { testQuery("select E() from movies") }
    it("Pi") { testQuery("select PI() from movies") }

    describe("shiftleft") {
      val f = "shiftleft"

      it(s"$f works with nullable column") {
        testQuery(s"select $f(id, floor(critic_rating)) as $f from movies")
      }
      it(s"$f with partial pushdown because of udf in the left argument") {
        testQuery(
          s"select $f(longIdentity(id), floor(critic_rating)) as $f from movies",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the right argument") {
        testQuery(
          s"select $f(id, floatIdentity(floor(critic_rating))) as $f from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("shiftright") {
      val f = "shiftright"

      it(s"$f works with nullable column") {
        testQuery(s"select $f(id, floor(critic_rating)) as $f from movies")
      }
      it(s"$f with partial pushdown because of udf in the left argument") {
        testQuery(
          s"select $f(longIdentity(id), floor(critic_rating)) as $f from movies",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the right argument") {
        testQuery(
          s"select $f(id, floatIdentity(floor(critic_rating))) as $f from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("shiftrightunsigned") {
      val f = "shiftrightunsigned"

      ignore(s"09/2024 - $f works with nullable column") {
        testQuery(s"select $f(id, floor(critic_rating)) as $f from movies")
      }
      ignore(s"09/2024 - $f with partial pushdown because of udf in the left argument") {
        testQuery(
          s"select $f(longIdentity(id), floor(critic_rating)) as $f from movies",
          expectPartialPushdown = true
        )
      }
      ignore(s"09/2024 - $f with partial pushdown because of udf in the right argument") {
        testQuery(
          s"select $f(id, floatIdentity(floor(critic_rating))) as $f from movies",
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("Decimal Expressions") {
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
             |select sum(cast(rating as decimal($precision, $scale))) over (order by rating) as out
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
             |select avg(cast(rating as decimal($precision, $scale))) over (order by rating) as out
             |from reviews
             |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
    }
  }

  describe("Aggregate Expressions") {
    val functionsGroup1 = Seq(
      "skewness",
      "kurtosis",
      "var_pop", "var_samp",
      "stddev_samp", "stddev_pop",
      "avg", "min", "max", "sum"
    ).sorted
    val singleStoreVersion = SinglestoreVersion(7, 6, 0)

    for (f <- functionsGroup1) {
      describe(f) {
        it (s"$f works with non-nullable int column") {
          f match {
            case "sum" =>
              // singlestore SUM returns DECIMAL(41, 0) which is not supported
              // by spark (spark maximum decimal precision is 38)
              testSingleReadForOldS2(s"select $f(age) as $f from users", singleStoreVersion)
            case _ =>
              testSingleReadForOldS2(s"select $f(user_id) as $f from reviews", singleStoreVersion)
          }
        }
        it(s"$f works with non-nullable float column") {
          testSingleReadForOldS2(
            s"select $f(cast(rating as decimal(2, 1))) as $f from reviews",
            singleStoreVersion
          )
        }
        it(s"$f works with nullable float column") {
          testSingleReadForOldS2(
            s"select $f(cast(critic_rating as decimal(2, 1))) as $f from movies",
            singleStoreVersion
          )
        }

        if (Seq("min", "max").contains(f)) {
          it(s"$f works with non-nullable string column") {
            testSingleReadForOldS2(s"select $f(first_name) as $f from users", singleStoreVersion)
          }
        }

        it(s"$f with partial pushdown because of udf") {
          testSingleReadQuery(
            s"select $f(longIdentity(user_id)) as $f from reviews",
            expectPartialPushdown = true
          )
        }
        it(s"$f works with filter") {
          testSingleReadForOldS2(
            s"select $f(age) filter (where age % 2 = 0) as $f from users",
            singleStoreVersion
          )
        }

        if (!Seq("min", "max").contains(f)) {
          it(s"$f works with filter for equal range population(std = 0)") {
            testSingleReadForOldS2(
              s"select $f(age) filter (where age = 60) as $f from users",
              SinglestoreVersion(7, 6, 0)
            )
          }
        }
      }
    }

    val functionsGroup2 = Seq("first", "last").sorted

    for (f <- functionsGroup2) {
      describe(f) {
        ignore(s"09/2024 - $f works with non-nullable string column") {
          testSingleReadForReadFromLeaves(s"select $f(first_name) as $f from users group by id")
        }
        ignore(s"09/2024 - $f with partial pushdown because of udf") {
          testSingleReadQuery(
            s"select $f(stringIdentity(first_name)) as $f from users group by id",
            expectPartialPushdown = true
          )
        }
        ignore(s"09/2024 - $f works with filter") {
          testSingleReadForReadFromLeaves(
            s"select $f(first_name) filter (where age % 2 = 0) as $f from users group by id"
          )
        }
      }
    }

    describe("count") {
      val f = "count"

      it(s"$f all") {
        testSingleReadForOldS2(s"select $f(*) as $f from users", singleStoreVersion)
      }
      it(s"$f distinct") {
        testSingleReadForOldS2(
          s"select $f(distinct first_name) as $f from users",
          singleStoreVersion
        )
      }
      it(s"$f (all) with partial pushdown because of udf") {
        testSingleReadQuery(
          s"select $f(stringIdentity(first_name)) as $f from users group by id",
          expectPartialPushdown = true
        )
      }
      it(s"$f (distinct) with partial pushdown because of udf") {
        testSingleReadQuery(
          s"select $f(distinct stringIdentity(first_name)) as $f from users group by id",
          expectPartialPushdown = true
        )
      }
      it(s"$f works with filter") {
        testSingleReadForOldS2(
          s"select $f(*) filter (where age % 2 = 0) as $f from users",
          singleStoreVersion
        )
      }
      it(s"$f with partial pushdown because of udf in filter") {
        testSingleReadQuery(
          s"select ${f}_if(age % 2 = 0) filter (where integerFilter(age)) as $f from users",
          expectPartialPushdown = true
        )
      }
      it(s"${f}_if") {
        testSingleReadForOldS2(s"select ${f}_if(age % 2 = 0) as $f from users", singleStoreVersion)
      }
      it(s"${f}_if works with filter") {
        testSingleReadForOldS2(
          s"select ${f}_if(age % 2 = 0) filter (where age % 2 = 0) as $f from users",
          singleStoreVersion
        )
      }
      it(s"$f top 3 email domains") {
        testOrderedQuery(
          s"""
            |select domain, $f(*)
            |from (
            | select substring(email, locate('@', email) + 1) as domain
            | from users
            |)
            |group by 1
            |order by 2 desc, 1 asc
            |limit 3
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
    }
  }

  describe("Window Functions") {
    val functionsGroup1 = Seq("rank", "row_number", "dense_rank").sorted

    for (f <- functionsGroup1) {
      it(s"$f order by works with non-nullable column") {
        testSingleReadForReadFromLeaves(
          s"select $f as ${f.head} from (select $f() over (order by first_name) as $f from users)"
        )
      }
      it(s"$f partition order by works with non-nullable column") {
        testSingleReadForReadFromLeaves(
          s"select $f() over (partition by first_name order by first_name) as $f from users"
        )
      }
    }

    val functionsGroup2 = Seq("lag", "lead").sorted

    for (f <- functionsGroup2) {
      it(s"$f order by works with non-nullable column") {
        testSingleReadForReadFromLeaves(
          s"select first_name, $f(first_name) over (order by first_name) as $f from users"
        )
      }
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

  describe("SortOrder") {
    val f = "SortOrder"

    it(s"$f works asc, nulls first") {
      testOrderedQuery("select * from movies order by critic_rating asc nulls first, id")
    }
    it(s"$f works desc, nulls last") {
      testOrderedQuery("select * from movies order by critic_rating desc nulls last, id")
    }
    it(s"$f partial pushdown asc, nulls last") {
      testSingleReadQuery(
        "select * from movies order by critic_rating asc nulls last",
        expectPartialPushdown = true
      )
    }
    it(s"$f partial pushdown desc, nulls first") {
      testSingleReadQuery(
        "select * from movies order by critic_rating desc nulls first",
        expectPartialPushdown = true
      )
    }
    it(s"$f with partial pushdown because of udf") {
      testSingleReadQuery(
        "select * from movies order by floatIdentity(critic_rating)",
        expectPartialPushdown = true
      )
    }
  }

  describe("Sort/Limit") {
    it("numeric order") { testOrderedQuery("select * from users order by id asc") }
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

  describe("Hash Expressions") {
    describe("sha1") {
      val f = "sha1"

      it(s"$f works with text") { testQuery(s"select $f(first_name) as $f from users") }
      it(s"$f works with literal null") {
        testSingleReadForReadFromLeaves(s"select $f(null) as $f from users")
      }
      it(s"$f with partial pushdown because of udf") {
        testQuery(
          s"""
            |select $f(stringIdentity(first_name)) as $f, stringIdentity(first_name) as first_name
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    describe("sha2") {
      val f = "sha2"

      it(s"$f works with 0 bit length") { testQuery(s"select $f(first_name, 0) as $f from users") }
      it(s"$f works with 256 bit length") {
        testQuery(s"select $f(first_name, 256) as $f from users")
      }
      it(s"$f works with 384 bit length") {
        testQuery(s"select $f(first_name, 384) as $f from users")
      }
      it(s"$f works with 512 bit length") {
        testQuery(s"select $f(first_name, 512) as $f from users")
      }
      it(s"$f works with literal null") {
        testSingleReadForReadFromLeaves(s"select $f(null, 256) as $f from users")
      }
      it(s"$f works with partial pushdown for 224 bit length") {
        testQuery(s"select sha2(first_name, 224) from users", expectPartialPushdown = true)
      }
      it(s"$f works with partial pushdown because of udf") {
        testQuery(
          s"""
            |select
            | $f(stringIdentity(first_name), 256) as $f,
            | stringIdentity(first_name) as first_name
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
      it(s"$f works with short literal") {
        testQuery(s"select $f(first_name, 256S) as $f from users")
      }
      it(s"$f works with long literal") {
        testQuery(s"select $f(first_name, 256L) as $f from users")
      }
    }

    describe("md5") {
      val f = "md5"

      it(s"$f works with text") { testQuery(s"select $f(first_name) as $f from users") }
      it(s"$f works with literal null") {
        testSingleReadForReadFromLeaves(s"select $f(null) as $f from users")
      }
      it(s"$f works with partial pushdown because of udf") {
        testQuery(
          s"""
            |select $f(stringIdentity(first_name)) as $f, stringIdentity(first_name) as first_name
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("Joins") {
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
            """
              |select *
              |from
              | users
              | left outer join
              | (select rating from reviews order by rating limit 10)
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
            "Join of relations with different JDBC connection options should not be pushed down"
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

  describe("Comparison Operators") {
    val functions = Seq(
      ("equal", "=", "1"),
      ("equalNullSafe", "<=>", "1"),
      ("lessThan", "<", "10"),
      ("lessThanOrEqual", "<=" , "10"),
      ("greaterThan", ">", "10"),
      ("greaterThanOrEqual", ">=", "10")
    ).sorted

    for ((f, s, v1) <- functions) {
      describe(f) {
        it(s"$f works with tinyint") { testQuery(s"select owns_house $s 1 as $f from users") }
        it(s"$f works with single brackets") { testQuery(s"select id $s '$v1' as $f from users") }
        it(s"$f works with boolean") {
          testQuery(
            s"""
              |select
              |  cast(owns_house as boolean) $s ${if (f == "greaterThan") false else true} as $f
              |from users
              |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
          )
        }
        it(s"$f works with tinyint $f null") {
          testQuery(s"select owns_house $s null as $f from users")
        }

        if (Seq("lessThan", "lessThanOrEqual", "greaterThan", "greaterThanOrEqual").contains(f)) {
          it(s"$f works with text") { testQuery(s"select first_name $s 'ZZZ' as $f from users") }
        }

        it(s"$f works with null $f null") { testQuery(s"select null $s null as $f from users") }
        it(s"$f with partial pushdown because of udf") {
          testQuery(
            s"select stringIdentity(id) $s '$v1' as $f from users",
            expectPartialPushdown = true
          )
        }
      }
    }
  }

  describe("Same-Name Column Selection") {
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

  describe("Regular Expressions") {
    describe("like") {
      val f = "like"

      it(s"$f works with simple literal") {
        testQuery(s"select * from users where first_name $f 'Di'")
      }
      it(s"$f works with simple non-nullable columns in both fields") {
        testQuery(s"select * from users where first_name $f last_name")
      }
      it(s"$f works with character wildcard") {
        testQuery(s"select * from users where first_name $f 'D_'")
      }
      it(s"$f works with string wildcard") {
        testQuery(s"select * from users where first_name $f 'D%'")
      }
      it(s"$f works with simple true") { testQuery(s"select * from users where 1 $f 1") }
      it(s"$f works with simple false") { testQuery(s"select 2 $f 1 from users") }
      it(s"$f works with simple true using non-nullable columns") {
        testQuery(s"select first_name from users where first_name $f first_name")
      }
      it(s"$f with partial pushdown because of udf on the left side") {
        testQuery(
          s"select * from users where stringIdentity(first_name) $f 'Di'",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf on the right side") {
        testQuery(
          s"select * from users where first_name $f stringIdentity('Di')",
          expectPartialPushdown = true
        )
      }
      it(s"$f works with null") { testQuery(s"select critic_review $f null from movies") }
      it(s"$f works with escape char") {
        testQuery(
          s"""
            |select
            | first_name $f last_name escape '^',
            | last_name $f first_name escape '^'
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it(s"$f works with escape char equal to '/'") {
        testQuery(
          s"""
            |select
            | first_name $f last_name escape '/',
            | last_name $f first_name escape '/'
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
    }

    describe("rlike") {
      val f = "rlike"

      it(s"$f works with simple literal") {
        testQuery(s"select * from users where first_name $f 'D.'")
      }
      it(s"$f works with simple non-nullable columns in both fields") {
        testQuery(s"select * from users where first_name $f last_name")
      }
      it(s"$f works with pattern match starting from beginning") {
        testQuery(s"select * from users where first_name $f '^D.'")
      }
      it(s"$f works with simple true") { testQuery(s"select * from users where 1 $f 1") }
      it(s"$f works with simple false") { testQuery(s"select 2 $f 1 from users") }
      it(s"$f with partial pushdown because of udf on the left side") {
        testQuery(
          s"select * from users where stringIdentity(first_name) $f 'D.'",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf on the right side") {
        testQuery(
          s"select * from users where first_name $f stringIdentity('D.')",
          expectPartialPushdown = true
        )
      }
      it(s"$f works with null") { testQuery(s"select critic_review $f null from movies") }
    }

    describe("(not) like all/any patterns functions") {
      val functions = Seq("like all", "like any", "not like all", "not like any").sorted

      for (f <- functions) {
        describe(f) {
          it(s"$f works with simple literal") {
            testQuery(s"select id, first_name from users where first_name $f ('An%te%')")
          }
          it(s"$f works with simple non-nullable column and literal") {
            testQuery(s"select id, first_name from users where first_name $f (last_name, 'Al%')")
          }
          it(s"$f works with repeated pattern match") {
            testQuery(
              s"select id, first_name from users where first_name $f ('Al%', last_name, 'Al%')"
            )
          }
          it(s"$f works with simple character wildcard") {
            testQuery(s"select * from users where first_name $f ('A___e', '_n__e')")
          }
          it(s"$f works with simple string wildcard") {
            testQuery(
              s"select * from users where first_name $f ('Kon%ce', '%tan%', '%Kon%tan%ce%')"
            )
          }

          f match {
            case "like all" | "like any" =>
              it(s"$f works with simple true") {
                testQuery(s"select * from users where '1' $f ('1')")
              }
              it(s"$f works with simple false") {
                testQuery(s"select * from users where id $f ('D%', 'A%bbbb%')", expectEmpty = true)
              }
              it(s"$f works with simple non-nullable column") {
                testQuery(s"select * from users where first_name $f (first_name)")
              }
            case "not like all" | "not like any" =>
              it(s"$f works with simple false") {
                testQuery(s"select * from users where id $f ('D%', 'A%bbbb%')")
              }
              it(s"$f works with simple non-nullable column") {
                testQuery(
                  s"select * from users where first_name $f (first_name)",
                  expectEmpty = true
                )
              }
          }

          it(s"$f works with null") {
            testQuery(s"select critic_review $f (null) from movies")
          }
          it(s"$f with partial pushdown because of udf on the left side") {
            testQuery(
              s"select * from users where stringIdentity(first_name) $f ('Ali%')",
              expectPartialPushdown = true
            )
          }
          it(s"$f with partial pushdown because of udf on the right side") {
            testQuery(
              s"select * from users where first_name $f (stringIdentity('Ali%'))",
              expectPartialPushdown = true
            )
          }
          it(s"$f works with very simple patterns") {
            spark.version.substring(0, 3) match {
              case "3.4" | "3.5" =>
                // Spark 3.4|3.5 invoke full pushdown
                testQuery(s"select * from users where first_name $f ('A%', '%b%', '%e')")
              case _ =>
                // Spark 3.1|3.2|3.3 compute these in more optimal way and do not invoke pushdown
                testQuery(
                  s"select * from users where first_name $f ('A%', '%b%', '%e')",
                  expectPartialPushdown = true
                )
            }
          }
          it(s"$f works with empty patterns arg") {
            try {
              testQuery(s"select * from users where first_name $f ()", expectPartialPushdown = true)
            } catch {
              case e: Throwable =>
                if (e.toString.contains("Expected something between '(' and ')'")) {
                  None
                } else {
                  throw e
                }
            }
          }
        }
      }
    }

    describe("regexp") {
      val f = "regexp"

      it(s"$f works with simple literal") {
        testQuery(s"select * from users where first_name $f 'D.'")
      }
      it(s"$f works with simple non-nullable columns in both fields") {
        testQuery(s"select * from users where first_name $f last_name")
      }
      it(s"$f works with simple true") { testQuery(s"select * from users where 1 $f 1") }
      it(s"$f works with simple false") { testQuery(s"select 2 $f 1 from users") }
      it(s"$f with partial pushdown because of udf on the left side") {
        testQuery(
          s"select * from users where stringIdentity(first_name) $f 'D.'",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf on the right side") {
        testQuery(
          s"select * from users where first_name $f stringIdentity('D.')",
          expectPartialPushdown = true
        )
      }
      it(s"$f works with null") { testQuery(s"select critic_review $f null from movies") }
    }

    describe("regexpReplace") {
      val f = "regexp_replace"

      it(s"$f works with simple literals") {
        testQuery(s"select $f(first_name, 'D', 'd') from users")
      }
      it(s"$f works correctly with simple literals in equality") {
        testQuery(s"select * from users where $f(first_name, 'D', 'd') = 'di'")
      }
      it(s"$f with partial pushdown because of udf") {
        testQuery(
          s"select $f(stringIdentity(first_name), 'D', 'd') from users",
          expectPartialPushdown = true
        )
      }
      it(s"$f works with null") { testQuery(s"select $f(title, 'D', critic_review) from movies") }
      it(s"$f works with simple non-nullable columns in fields") {
        testQuery(s"select $f(first_name, first_name, first_name) from users")
      }
      it(s"$f works with small position argument") {
        testQuery(s"select $f(first_name, 'a', 'd', 3) from users")
      }
      it(s"$f works with big position argument") {
        testQuery(s"select $f(first_name, 'a', 'd', 100) from users")
      }
    }
  }

  describe("Datetime Expressions") {
    val functionsGroup1 = Seq(
      ("DateAdd", "date_add"),
      ("DateSub", "date_sub")
    ).sorted

    for ((f, s) <- functionsGroup1) {
      describe(f) {
        it(s"$f works with positive num_days") { testQuery(s"select $s(birthday, age) from users") }
        it(s"$f works with negative num_days") { testQuery(s"select $s(birthday, -age) from users") }
        it(s"$f with partial pushdown because of udf in the left argument") {
          testQuery(
            s"select $s(stringIdentity(birthday), age) as ${f.toLowerCase} from users",
            expectPartialPushdown = true
          )
        }
        it(s"$f with partial pushdown because of udf in the right argument") {
          testQuery(
            s"select $s(birthday, -cast(longIdentity(age) as int)) as ${f.toLowerCase} from users",
            expectPartialPushdown = true
          )
        }
      }
    }

    val functionsGroup2 = Seq(
      ("toUnixTimestamp", "to_unix_timestamp"),
      ("unixTimestamp", "unix_timestamp")
    ).sorted

    for ((f, s) <- functionsGroup2) {
      describe(f) {
        it(s"$f works with TimestampType") {
          testQuery(s"select created, $s(created) from reviews")
        }
        it(s"$f works with DateType") { testQuery(s"select birthday, $s(birthday) from users") }
        it(s"$f with partial pushdown because of udf") {
          testQuery(
            s"select $s(stringIdentity(birthday)) as ${f.toLowerCase} from users",
            expectPartialPushdown = true
          )
        }
      }
    }

    describe("fromUnixTime") {
      val (f, s) = ("fromUnixTime", "from_unixtime")

      it(s"$f works with non-nullable column") {
        // cast is needed because in SingleStore 6.8 FROM_UNIXTIME query returns a result with microseconds
        testQuery(s"select id, cast($s(id) as timestamp) as ${f.toLowerCase} from movies")
      }
      it(s"$f with partial pushdown because of udf") {
        testQuery(
          s"select $s(stringIdentity(id)) as ${f.toLowerCase} from movies",
          expectPartialPushdown = true
        )
      }
    }

    // SingleStore and Spark differ on how they do last day
    // calculations, so we ignore them in some of these tests

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
    ).sorted

    describe("timeAdd") {
      val (f, s) = ("timeAdd", "+")

      for (interval <- intervals) {
        it(s"$f works with simple interval $interval") {
          if (!interval.contains("day") || spark.version != "3.0.0") {
            testQuery(
              s"""
                |select
                | created,
                | created $s interval $interval as ${f.toLowerCase}
                |from reviews
                |where date(created) != last_day(created)
                |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
            )
          }
        }
      }

      it(s"$f with partial pushdown because of udf in the left argument") {
        testQuery(
          s"""
             |select
             | created,
             | to_timestamp(stringIdentity(created)) $s interval 1 month as ${f.toLowerCase}
             |from reviews
             |where date(created) != last_day(created)
             |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    describe("timeSub") {
      val (f, s) = ("timeSub", "-")

      for (interval <- intervals) {
        it(s"$f works with simple interval $interval") {
          testQuery(
            s"""
              |select
              | created,
              | created $s interval $interval as ${f.toLowerCase}
              |from reviews
              |where date(created) != last_day(created)
              |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
          )
        }
      }

      it(s"$f with partial pushdown because of udf in the left argument") {
        testQuery(
          s"""
            |select
            | created,
            | to_timestamp(stringIdentity(created)) $s interval 1 day as ${f.toLowerCase}
            |from reviews
            |where date(created) != last_day(created)
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    describe("addMonths") {
      val (f, s) = ("addMonths", "add_months")

      val numMonthsList = Seq(0, 1, 2, 12, 13, 200, -1, -2, -12, -13, -200).sorted
      for (numMonths <- numMonthsList) {
        it(s"$f works with simple literal $numMonths months") {
          testQuery(
            s"""
              |select created, $s(created, $numMonths) as ${f.toLowerCase}
              |from reviews
              |where date(created) != last_day(created)
              |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
          )
        }
      }

      it(s"$f with partial pushdown because of udf in the left argument") {
        testQuery(
          s"""
            |select created, $s(stringIdentity(created), 1) as ${f.toLowerCase}
            |from reviews
            |where date(created) != last_day(created)
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the right argument") {
        testQuery(
          s"""
             |select created, $s(created, stringIdentity(1)) as ${f.toLowerCase}
             |from reviews
             |where date(created) != last_day(created)
             |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    describe("nextDay") {
      val (f, s) = ("nextDay", "next_day")

      for ((dayOfWeek, _) <- ExpressionGen.DAYS_OF_WEEK_OFFSET_MAP) {
        it(s"$f works with dayOfWeek: $dayOfWeek") {
          testQuery(
            s"select created, $s(created, '$dayOfWeek') as ${f.toLowerCase} from reviews"
          )
        }
      }

      it(s"$f works with invalid dayOfWeek name") {
        testQuery(
          s"select created, $s(created, 'invalid_day') as ${f.toLowerCase} from reviews"
        )
      }
      it(s"$f with partial pushdown because of udf in the left argument") {
        testQuery(
          s"select created, $s(stringIdentity(created), 'monday') as ${f.toLowerCase} from reviews",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the right argument") {
        testQuery(
          s"select created, $s(created, stringIdentity('monday')) as ${f.toLowerCase} from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("datediff") {
      val f = "datediff"

      it(s"$f works simple non-nullable columns") {
        testSingleReadForReadFromLeaves(
          s"""
            |select
            | birthday,
            | created,
            | $f(birthday, created) as ${f}1,
            | $f(created, birthday) as ${f}2,
            | $f(created, created) as ${f}3
            |from
            | users
            | inner join
            | reviews
            | on users.id = reviews.user_id
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it(s"$f with partial pushdown because of udf in the left argument") {
        testQuery(
          s"select $f(stringIdentity(created), created) as $f from reviews",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the right argument") {
        testQuery(
          s"select $f(created, stringIdentity(created)) as $f from reviews",
          expectPartialPushdown = true
        )
      }
    }

    // Spark doesn't support explicit time intervals like `+/-hh:mm`

    val timeZones = Seq("US/Mountain", "Asia/Seoul", "UTC", "EST", "Etc/GMT-6").sorted

    val functionsGroup3 = Seq(
      ("fromUTCTimestamp", "from_utc_timestamp"),
      ("toUTCTimestamp", "to_utc_timestamp")
    ).sorted

    for ((f, s) <- functionsGroup3) {
      describe(f) {
        for (timeZone <- timeZones) {
          it(s"$f works with timezone $timeZone") {
            testQuery(
              s"select $s(created, '$timeZone') as ${f.toLowerCase} from reviews" +
                // singlestore doesn't support timestamps less then 1970-01-01T00:00:00Z
                {
                  if (s == "to_utc_timestamp") {
                    s" where to_unix_timestamp(created) > 24*60*60"
                  } else { "" }
                }
            )
          }
        }
        it(s"$f with partial pushdown because of udf in the left argument") {
          testQuery(
            s"select $s(stringIdentity(created), 'EST') as ${f.toLowerCase} from reviews",
            expectPartialPushdown = true
          )
        }
        it(s"$f with partial pushdown because of udf in the right argument") {
          testQuery(
            s"select $s(created, stringIdentity('EST')) as ${f.toLowerCase} from reviews",
            expectPartialPushdown = true
          )
        }
      }
    }

    // TruncTimestamp is called as date_trunc() in Spark
    describe("truncTimestamp") {
      val dateParts = Seq(
        "YEAR", "YYYY", "YY",
        "MON", "MONTH", "MM",
        "DAY", "DD",
        "HOUR", "MINUTE", "SECOND",
        "WEEK", "QUARTER"
      ).sorted
      val (f, s) = ("truncTimestamp", "date_trunc")

      for (datePart <- dateParts) {
        it(s"$f works with datepart $datePart") {
          testQuery(s"select $s('$datePart', created) as ${f.toLowerCase} from reviews")
        }
      }

      it(s"$f with partial pushdown because of udf in the left argument") {
        testQuery(
          s"select $s(stringIdentity('DAY'), created) as ${f.toLowerCase} from reviews",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the right argument") {
        testQuery(
          s"select $s('DAY', stringIdentity(created)) as ${f.toLowerCase} from reviews",
          expectPartialPushdown = true
        )
      }
    }

    // TruncDate is called as trunc() in Spark
    describe("truncDate") {
      val dateParts = Seq("YEAR", "YYYY", "YY", "MON", "MONTH", "MM").sorted
      val (f, s) = ("truncDate", "trunc")

      for (datePart <- dateParts) {
        it(s"$f works with datepart $datePart") {
          testQuery(s"select $s(created, '$datePart') as ${f.toLowerCase} from reviews")
        }
      }
      it(s"$f with partial pushdown because of udf in the left argument") {
        testQuery(
          s"select $s(stringIdentity(created), 'MONTH') as ${f.toLowerCase} from reviews",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the right argument") {
        testQuery(
          s"select $s(created, stringIdentity('MONTH')) as ${f.toLowerCase} from reviews",
          expectPartialPushdown = true
        )
      }
    }

    describe("monthsBetween") {
      val (f, s) = ("monthsBetween", "months_between")

      for (interval <- intervals) {
        it(s"$f works with interval $interval") {
          testQuery(
            s"select $s(created, created + interval $interval) as ${f.toLowerCase} from reviews"
          )
        }
      }
      it(s"$f with partial pushdown because of udf in the left argument") {
        testQuery(
          s"""
            |select
            | $s(stringIdentity(created), created + interval 1 month) as ${f.toLowerCase}
            |from reviews
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the right argument") {
        testQuery(
          s"""
             |select
             | $s(
             |  created,
             |  to_timestamp(stringIdentity(created)) + interval 1 month
             | ) as ${f.toLowerCase}
             |from reviews
             |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    val periodsList: Seq[Seq[String]] = Seq(
      Seq("YEAR", "Y", "YEARS", "YR", "YRS"),
      Seq("QUARTER", "QTR"),
      Seq("MONTH", "MON", "MONS", "MONTHS"),
      Seq("WEEK", "W", "WEEKS"),
      Seq("DAY", "D", "DAYS"),
      Seq("DAYOFWEEK", "DOW"),
      Seq("DAYOFWEEK_ISO", "DOW_ISO"),
      Seq("DOY"),
      Seq("HOUR", "H", "HOURS", "HR", "HRS"),
      Seq("MINUTE", "MIN", "M", "MINS", "MINUTES")
    ).map(_.sorted)

    describe("extract") {
      val f = "extract"

      for (periods <- periodsList) {
        for (period <- periods) {
          it(s"$f works with period $period") {
            testQuery(s"select $f($period from birthday) as $f from users")
            testQuery(s"select $f($period from created) as $f from reviews")
          }
        }
      }
    }

    describe("datePart") {
      val (f, s) = ("datePart", "date_part")

      for (periods <- periodsList) {
        for (period <- periods) {
          it(s"$f works with period $period") {
            testQuery(s"select $s('$period', birthday) as ${f.toLowerCase} from users")
            testQuery(s"select $s('$period', created) as ${f.toLowerCase} from reviews")
          }
        }
      }
    }

    it("makeDate") {
      testQuery("select make_date(1000, user_id, user_id) from reviews")
    }
    it("makeTimestamp") {
      testQuery(
        "select make_timestamp(1000, user_id, user_id, user_id, user_id, user_id) from reviews"
      )
    }
    it("CurrentDate") { testQuery("select current_date() from users", expectSameResult = false) }
    it("Now") { testQuery("select now() from users", expectSameResult = false) }
    it("DateFromUnixDate") { testQuery("select date_from_unix_date(1234567) from users") }
    it("CurrentTimestamp") {
      testQuery("select current_timestamp() from users", expectSameResult = false)
    }

    describe("UnixDate") {
      val (f, s) = ("UnixDate", "unix_date")

      it(s"$f works with the simple query of one day after epoch") {
        testQuery(s"select $s(date('1970-01-02')) as ${f.toLowerCase} from users")
      }
      it(s"$f works with non-nullable column") {
        testQuery(s"select $s(birthday) as ${f.toLowerCase} from users")
      }
      it(s"$f with partial pushdown because of udf") {
        testQuery(
          s"select $s(date(stringIdentity(birthday))) as ${f.toLowerCase} from users",
          expectPartialPushdown = true
        )
      }
    }

    describe("Unix Seconds Functions") {
      val functions = Seq("unix_seconds", "unix_micros", "unix_millis").sorted

      for (f <- functions) {
        it(s"$f works with timestamp") {
          testQuery(s"select $f(created) as ${f.toLowerCase.replace("_", "")} from reviews")
        }
        it(s"$f with partial pushdown because of udf") {
          testQuery(
            s"""
               |select
               |  $f(timestamp(stringIdentity(created))) as ${f.toLowerCase.replace("_", "")}
               |from reviews
               |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
      }
    }

    describe("Converting (milli/micro)seconds to Timestamp Functions") {
      val functions = Seq("timestamp_seconds", "timestamp_millis", "timestamp_micros").sorted

      for (f <- functions) {
        it(s"$f works with non-nullable column") {
          testQuery(
            s"select created, $f(user_id) as ${f.toLowerCase.replace("_", "")} from reviews"
          )
        }
        it(s"$f works with negative non-nullable column") {
          testQuery(s"select $f(-movie_id) as ${f.toLowerCase.replace("_", "")} from reviews")
        }
        it(s"$f with partial pushdown because of udf") {
          testQuery(
            s"select $f(int(stringIdentity(id))) as ${f.toLowerCase.replace("_", "")} from users",
            expectPartialPushdown = true
          )
        }
      }
    }

    describe("Timestamp Parts Functions") {
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
      ).sorted

      for (f <- functions) {
        it(s"$f works with date non-nullable column") {
          testQuery(s"select $f(birthday) as ${f.toLowerCase} from users")
        }
        it(s"$f works with timestamp non-nullable column") {
          testQuery(s"select $f(created) as ${f.toLowerCase} from reviews")
        }
        it(s"$f works with string non-nullable column") {
          testQuery(s"select $f(first_name) as ${f.toLowerCase} from users")
        }
        it(s"$f with partial pushdown because of udf") {
          testQuery(
            s"select $f(stringIdentity(first_name)) as ${f.toLowerCase} from users",
            expectPartialPushdown = true
          )
        }
      }
    }
  }

  describe("String Expressions") {
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

  describe("JSON Functions") {
    describe("GetJsonObject") {
      val (f, s) = ("GetJsonObject", "get_json_object")

      it(s"$f works with string non-nullable columns") {
        testQuery(
          s"select id, $s(movie_rating, '$$.title') as ${f.toLowerCase} from movies_rating"
        )
      }
      it(s"$f works with int non-nullable columns") {
        testQuery(
          s"select id, $s(movie_rating, '$$.movie_id') as ${f.toLowerCase} from movies_rating"
        )
      }
      it(s"$f works with boolean non-nullable columns") {
        testQuery(s"select id, $s(movie_rating, '$$.3D') as ${f.toLowerCase} from movies_rating")
      }
      it(s"$f works with array non-nullable columns") {
        testQuery(
          s"select id, $s(movie_rating, '$$.reviews') as ${f.toLowerCase} from movies_rating"
        )
      }
      it(s"$f works with nested json non-nullable columns") {
        testQuery(
          s"select id, $s(movie_rating, '$$.timetable.hall') as ${f.toLowerCase} from movies_rating"
        )
      }
      it(s"$f works with nested json string non-nullable columns") {
        testQuery(
          s"select id, $s(movie_rating, '$$.timetable.day') as ${f.toLowerCase} from movies_rating"
        )
      }
      it(s"$f works with non-existing path") {
        testQuery(
          s"select id, $s(movie_rating, '$$.nonexistingPath') as ${f.toLowerCase} from movies_rating"
        )
      }
      it(s"$f works invalid path") {
        testQuery(
          s"select id, $s(movie_rating, 'rating') as ${f.toLowerCase} from movies_rating",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the first argument") {
        testQuery(
          s"""
            |select
            | id,
            | $s(stringIdentity(movie_rating), '$$.title') as ${f.toLowerCase}
            |from movies_rating
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf in the second argument") {
        testQuery(
          s"""
            |select
            | id,
            | $s(movie_rating, stringIdentity('$$.title')) as ${f.toLowerCase}
            |from movies_rating
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }

    describe("LengthOfJsonArray") {
      val (f, s) = ("LengthOfJsonArray", "json_array_length")

      it(s"$f works with simple non-nullable column") {
        testQuery(s"select id, $s(same_rate_movies) as ${f.toLowerCase} from movies_rating")
      }
      it(s"$f with partial pushdown because of udf") {
        testQuery(
          s"select id, $s(stringIdentity(same_rate_movies)) as ${f.toLowerCase} from movies_rating",
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("Misc Functions") {

    describe("UUID") {
      val f = "uuid"

      it(s"$f pushes down the correct query") {
        if (version.atLeast(SinglestoreVersion(7, 5, 0))) {
          testQuery(s"select id, $f() from users_sample", expectSameResult = false)
          testUUIDPushdown(s"select id, $f() from users_sample")
        } else {
          testQuery(
            s"select id, $f() from users_sample",
            expectSameResult = false,
            expectPartialPushdown = true
          )
        }
      }
      it(s"$f with partial pushdown because of udf") {
        testQuery(
          s"select $f(), stringIdentity(first_name) as first_name from users",
          expectPartialPushdown = true,
          expectSameResult = false
        )
        testUUIDPushdown(
          s"select $f(), stringIdentity(first_name) as first_name from users"
        )
      }
    }

    describe("Rand") {
      val f = "rand"

      it(s"$f works with literal integer and non-nullable column") {
        testQuery(s"select $f(100)*id as $f from users", expectSameResult = false)
      }
      it(s"$f works with literal long and non-nullable column") {
        testQuery(s"select $f(100L)*id as $f from users", expectSameResult = false)
      }
      it(s"$f works with literal null and non-nullable column") {
        testQuery(s"select $f(null)*id as $f from users", expectSameResult = false)
      }
      it(
        s"$f works with empty arguments and non-nullable column",
        ExcludeFromSpark31,
        ExcludeFromSpark32,
        ExcludeFromSpark33,
        ExcludeFromSpark34,
        ExcludeFromSpark35
      ) {
        // TODO PLAT-5759
        testQuery(
          s"select $f()*id as $f from users",
          expectSameResult = false,
          expectCodegenDeterminism = false
        )
      }
      it(s"$f should return the same value for the same input") {
        val df1 =
          spark.sql(s"select $f(100)*id as $f from (select id from testdb.users order by id)")
        val df2 =
          spark.sql(s"select $f(100)*id as $f from (select id from testdb.users order by id)")
        assertApproximateDataFrameEquality(df1, df2, 0.001, orderedComparison = false)
      }
    }
  }
}
