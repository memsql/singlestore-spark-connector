package com.memsql.spark

import com.memsql.spark.SQLGen.{MemsqlVersion, Relation}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class SQLPushdownTest extends IntegrationSuiteBase with BeforeAndAfterEach with BeforeAndAfterAll {

  override def beforeAll() = {
    super.beforeAll()
    super.beforeEach() // we want to run beforeEach to set up a spark session

    // need to specific explicit schemas - otherwise Spark will infer them
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
        :: Nil)

    writeTable("testdb.users",
               spark.read.schema(usersSchema).json("src/test/resources/data/users.json"))

    val moviesSchema = StructType(
      StructField("id", LongType)
        :: StructField("title", StringType)
        :: StructField("genre", StringType)
        :: StructField("critic_review", StringType, nullable = true)
        :: StructField("critic_rating", FloatType, nullable = true)
        :: Nil)

    writeTable("testdb.movies",
               spark.read.schema(moviesSchema).json("src/test/resources/data/movies.json"))

    val reviewsSchema = StructType(
      StructField("user_id", LongType)
        :: StructField("movie_id", LongType)
        :: StructField("rating", FloatType)
        :: StructField("review", StringType)
        :: StructField("created", TimestampType)
        :: Nil)

    writeTable("testdb.reviews",
               spark.read.schema(reviewsSchema).json("src/test/resources/data/reviews.json"))

    writeTable("testdb.users_sample",
               spark.read
                 .format("memsql")
                 .load("testdb.users")
                 .sample(0.5)
                 .limit(10))
  }

  override def beforeEach() = {
    super.beforeEach()

    spark.sql("create database testdb")
    spark.sql("create database testdb_nopushdown")
    spark.sql("create database testdb_jdbc")

    def makeTables(sourceTable: String) = {
      spark.sql(
        s"create table testdb.${sourceTable} using memsql options ('dbtable'='testdb.${sourceTable}')")
      spark.sql(
        s"create table testdb_nopushdown.${sourceTable} using memsql options ('dbtable'='testdb.${sourceTable}','disablePushdown'='true')")
      spark.sql(s"create table testdb_jdbc.${sourceTable} using jdbc options (${jdbcOptionsSQL(
        s"testdb.${sourceTable}")})")
    }

    makeTables("users")
    makeTables("users_sample")
    makeTables("movies")
    makeTables("reviews")
  }

  def extractQueriesFromPlan(root: LogicalPlan): Seq[String] = {
    root
      .map({
        case Relation(relation) => relation.sql
        case _                  => ""
      })
      .sorted
  }

  def testCodegenDeterminism(q: String): Unit = {
    val logManager    = LogManager.getLogger("com.memsql.spark")
    var setLogToTrace = false

    if (logManager.isTraceEnabled) {
      logManager.setLevel(Level.DEBUG)
      setLogToTrace = true
    }

    assert(
      extractQueriesFromPlan(spark.sql(q).queryExecution.optimizedPlan) ==
        extractQueriesFromPlan(spark.sql(q).queryExecution.optimizedPlan),
      "All generated MemSQL queries should be the same"
    )

    if (setLogToTrace) {
      logManager.setLevel(Level.TRACE)
    }
  }

  def testQuery(q: String,
                alreadyOrdered: Boolean = false,
                expectPartialPushdown: Boolean = false,
                expectSingleRead: Boolean = false,
                expectEmpty: Boolean = false,
                pushdown: Boolean = true): Unit = {

    spark.sql("use testdb_jdbc")
    val jdbcDF = spark.sql(q)
    // verify that the jdbc DF works first
    jdbcDF.collect()
    if (pushdown) { spark.sql("use testdb") } else { spark.sql("use testdb_nopushdown") }

    testCodegenDeterminism(q)

    val memsqlDF = spark.sql(q)

    if (!continuousIntegration) { memsqlDF.show(4) }

    if (expectEmpty) {
      assert(memsqlDF.count == 0, "result is expected to be empty")
    } else {
      assert(memsqlDF.count > 0, "result is expected to not be empty")
    }

    if (expectSingleRead) {
      assert(memsqlDF.rdd.getNumPartitions == 1,
             "query is expected to read from a single partition")
    } else {
      assert(memsqlDF.rdd.getNumPartitions > 1,
             "query is expected to read from multiple partitions")
    }

    assert(
      (memsqlDF.queryExecution.optimizedPlan match {
        case SQLGen.Relation(_) => false
        case _                  => true
      }) == expectPartialPushdown,
      s"the optimized plan does not match expectPartialPushdown=$expectPartialPushdown"
    )

    try {
      def changeTypes(df: DataFrame): DataFrame = {
        var newDf = df
        df.schema
          .filter(_.dataType == ShortType)
          .foreach(x => newDf = newDf.withColumn(x.name, newDf(x.name).cast(IntegerType)))
        df.schema
          .filter(_.dataType == FloatType)
          .foreach(x => newDf = newDf.withColumn(x.name, newDf(x.name).cast(DoubleType)))
        newDf
      }
      assertApproximateDataFrameEquality(changeTypes(memsqlDF),
                                         jdbcDF,
                                         0.1,
                                         orderedComparison = alreadyOrdered)
    } catch {
      case e: Throwable => {
        if (continuousIntegration) { println(memsqlDF.explain(true)) }
        throw e
      }
    }
  }

  def testOrderedQuery(q: String,
                       expectPartialPushdown: Boolean = false,
                       pushdown: Boolean = true): Unit = {
    // order by in MemSQL requires single read
    testQuery(q,
              alreadyOrdered = true,
              expectPartialPushdown = expectPartialPushdown,
              expectSingleRead = true,
              pushdown = pushdown)
  }

  def testSingleReadQuery(q: String,
                          alreadyOrdered: Boolean = false,
                          expectPartialPushdown: Boolean = false,
                          pushdown: Boolean = true): Unit = {
    testQuery(q,
              alreadyOrdered = alreadyOrdered,
              expectPartialPushdown = expectPartialPushdown,
              expectSingleRead = true,
              pushdown = pushdown)
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
      testNoPushdownQuery("select * from users as a, reviews as b where a.id = b.user_id",
                          expectSingleRead = true)
    }
  }

  describe("sanity test the tables") {
    it("select all users") { testQuery("select * from users") }
    it("select all users (sampled)") { testQuery("select * from users_sample") }
    it("select all movies") { testQuery("select * from movies") }
    it("select all reviews") { testQuery("select * from reviews") }
  }

  describe("math expressions") {
    it("sinh") { testQuery("select sinh(rating) as sinh from reviews") }
    it("cosh") { testQuery("select cosh(rating) as cosh from reviews") }
    it("tanh") { testQuery("select tanh(rating) as tanh from reviews") }
    it("hypot") { testQuery("select hypot(rating, user_id) as hypot from reviews") }
    it("rint") { testQuery("select rint(rating) as rint from reviews") }
    it("asinh") { testQuery("select asinh(rating) as asinh from reviews") }
    it("acosh") { testQuery("select acosh(rating) as acosh from reviews") }
    it("atanh") {
      testQuery(
        "select atanh(critic_rating) as atanh from movies where critic_rating > -1 AND critic_rating < 1")
    }
    it("integralDivide") { testQuery("SELECT user_id div movie_id FROM reviews") }
  }

  describe("bit operations") {
    it("bit_count") { testQuery("SELECT bit_count(user_id) AS bit_count FROM reviews") }
    def bitOperationTest(sql: String): Unit = {
      val bitOperationsMinVersion = MemsqlVersion(7, 0, 1)
      val resultSet = jdbcConnection.to(conn =>
        Loan(conn.createStatement()).to(_.executeQuery("select @@memsql_version")))
      resultSet.next()
      val version = MemsqlVersion(resultSet.getString("@@memsql_version"))
      if (version.atLeast(bitOperationsMinVersion))
        testSingleReadQuery(sql)
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
    it("null literal") { testSingleReadQuery("select null from users") }
    it("int literal") { testQuery("select 1 from users") }
    it("bool literal") { testQuery("select true from users") }
    it("float, bool literal") { testQuery("select 1.2 as x, true from users") }

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
      testSingleReadQuery("SELECT typeof(user_id), typeof(created), typeof(review) FROM reviews")
    }
  }

  describe("filter") {
    it("numeric equality") { testQuery("select * from users where id = 1") }
    it("numeric inequality") { testQuery("select * from users where id != 1") }
    it("numeric comparison >") { testQuery("select * from users where id > 500") }
    it("numeric comparison > <") { testQuery("select * from users where id > 500 and id < 550") }
    it("string equality") { testQuery("select * from users where first_name = 'Evan'") }
  }

  describe("aggregations") {
    it("count") { testSingleReadQuery("select count(*) from users") }
    it("count filter") {
      testSingleReadQuery("select count(*) filter (where age % 2 = 0) from users")
    }
    it("count filter udf") {
      spark.udf.register("myUDF", (x: Int) => x % 3 == 1)
      testSingleReadQuery("SELECT count_if(age % 2 = 0) filter (where myUDF(age)) FROM users",
                          expectPartialPushdown = true)
    }
    it("count_if") { testSingleReadQuery("SELECT count_if(age % 2 = 0) as count FROM users") }
    it("count_if filter") {
      testSingleReadQuery("SELECT count_if(age % 2 = 0) filter (where age % 2 = 0) FROM users")
    }
    it("count distinct") { testSingleReadQuery("select count(distinct first_name) from users") }
    it("first") { testSingleReadQuery("select first(first_name) from users group by id") }
    it("first filter") {
      testSingleReadQuery(
        "select first(first_name) filter (where age % 2 = 0) from users group by id")
    }
    it("last") { testSingleReadQuery("select last(first_name) from users group by id") }
    it("last filter") {
      testSingleReadQuery(
        "select last(first_name) filter (where age % 2 = 0) from users group by id")
    }
    it("stddev_pop") { testSingleReadQuery("select stddev_pop(age) from users") }
    it("stddev_pop filter") {
      testSingleReadQuery("select stddev_pop(age) filter (where age % 2 = 0) from users")
    }
    it("stddev_samp") { testSingleReadQuery("select stddev_pop(age) from users") }
    it("stddev_samp filter") {
      testSingleReadQuery("select stddev_samp(age) filter (where age % 2 = 0) from users")
    }
    it("var_pop") { testSingleReadQuery("select var_pop(age) from users") }
    it("var_pop filter") {
      testSingleReadQuery("select var_pop(age) filter (where age % 2 = 0) from users")
    }
    it("var_samp") { testSingleReadQuery("select var_samp(age) from users") }
    it("var_samp filter") {
      testSingleReadQuery("select var_samp(age) filter (where age % 2 = 0) from users")
    }
    it("floor(avg(age))") { testSingleReadQuery("select floor(avg(age)) from users") }
    it("avg(age) filter") {
      testSingleReadQuery("select avg(age) filter (where age % 2 = 0) from users")
    }
    it("top 3 email domains") {
      testOrderedQuery(
        """
        |   select domain, count(*) from (
        |     select substring(email, locate('@', email) + 1) as domain
        |     from users
        |   )
        |   group by 1
        |   order by 2 desc, 1 asc
        |   limit 3
        |""".stripMargin
      )
    }
    it("max") { testSingleReadQuery("select max(user_id) as maxid from reviews") }
    it("max filter") {
      testSingleReadQuery(
        "select max(user_id) filter (where user_id % 2 = 0) as maxid_filter from reviews")
    }
    it("min") { testSingleReadQuery("select min(user_id) as minid from reviews") }
    it("min filter") {
      testSingleReadQuery(
        "select min(user_id) filter (where user_id % 2 = 0) as minid_filter from reviews")
    }
  }

  describe("window functions") {
    it("rank order by") {
      testSingleReadQuery(
        "select out as a from (select rank() over (order by first_name) as out from users)")
    }
    it("rank partition order by") {
      testSingleReadQuery(
        "select rank() over (partition by first_name order by first_name) as out from users")
    }
    it("row_number order by") {
      testSingleReadQuery("select row_number() over (order by first_name) as out from users")
    }
    it("dense_rank order by") {
      testSingleReadQuery("select dense_rank() over (order by first_name) as out from users")
    }
    it("lag order by") {
      testSingleReadQuery(
        "select first_name, lag(first_name) over (order by first_name) as out from users")
    }
    it("lead order by") {
      testSingleReadQuery(
        "select first_name, lead(first_name) over (order by first_name) as out from users")
    }
    it("ntile(3) order by") {
      testSingleReadQuery(
        "select first_name, ntile(3) over (order by first_name) as out from users")
    }
    it("percent_rank order by") {
      testSingleReadQuery(
        "select first_name, percent_rank() over (order by first_name) as out from users")
    }
  }

  describe("sort/limit") {
    it("numeric order") { testOrderedQuery("select * from users order by id asc") }
    it("text order") {
      testOrderedQuery("select * from users order by first_name desc, last_name asc, id asc")
    }
    it("text order expression") {
      testOrderedQuery("select * from users order by `email` like '%@gmail%', id asc")
    }

    it("text order case") {
      testOrderedQuery(
        "select * from users where first_name in ('Abbey', 'a') order by first_name desc, id asc")
    }

    it("simple limit") { testOrderedQuery("select 'a' from users limit 10") }
    it("limit with sort") { testOrderedQuery("select * from users order by id limit 10") }
    it("limit with sort on inside") {
      testOrderedQuery("select * from (select * from users order by id) limit 10")
    }
    it("limit with sort on outside") {
      testOrderedQuery("select * from (select * from users order by id limit 10) order by id")
    }
  }

  describe("joins") {
    it("implicit inner join") {
      testSingleReadQuery("select * from users as a, reviews where a.id = reviews.user_id")
    }
    it("explicit inner join") {
      testSingleReadQuery("select * from users inner join reviews on users.id = reviews.user_id")
    }
    it("cross join") {
      testSingleReadQuery("select * from users cross join reviews on users.id = reviews.user_id")
    }
    it("left outer join") {
      testSingleReadQuery(
        "select * from users left outer join reviews on users.id = reviews.user_id")
    }
    it("right outer join") {
      testSingleReadQuery(
        "select * from users right outer join reviews on users.id = reviews.user_id")
    }
    it("full outer join") {
      testSingleReadQuery(
        "select * from users full outer join reviews on users.id = reviews.user_id")
    }
    it("natural join") {
      testSingleReadQuery(
        "select users.id, rating from users natural join (select user_id as id, rating from reviews)")
    }
    it("complex join") {
      testSingleReadQuery(
        """
          |  select users.id, round(avg(rating), 2) as rating, count(*) as num_reviews
          |  from users inner join reviews on users.id = reviews.user_id
          | group by users.id
          |""".stripMargin)
    }
  }

  describe("same-name column selection") {
    it("join two tables which project the same column name") {
      testOrderedQuery(
        "select * from (select id from users) as a, (select id from movies) as b where a.id = b.id order by a.id")
    }
    it("select same columns twice via natural join") {
      testOrderedQuery("select * from users as a natural join users order by a.id")
    }
    it("select same column twice from table") {
      testQuery("select first_name, first_name from users", expectPartialPushdown = true)
    }
    it("select same column twice from table with aliases") {
      testOrderedQuery("select first_name as a, first_name as a from users order by id")
    }
    it("select same alias twice (different column) from table") {
      testOrderedQuery("select first_name as a, last_name as a from users order by id")
    }
    it("select same column twice in subquery") {
      testQuery("select * from (select first_name, first_name from users) as x",
                expectPartialPushdown = true)
    }
    it("select same column twice from subquery with aliases") {
      testOrderedQuery(
        "select * from (select first_name as a, first_name as a from users order by id) as x")
    }
  }

  describe("datetime expressions") {
    val intervals = List(
      "1 month",
      "3 week",
      "2 day",
      "7 hour",
      "3 minute",
      "5 second",
      "1 month 1 week",
      "2 month 2 hour",
      "3 month 1 week 3 hour 5 minute 4 seconds"
    )

    it("toUnixTimestamp with TimestampType") {
      testQuery("select created, to_unix_timestamp(created) from reviews")
    }

    it("toUnixTimestamp with DateType") {
      testQuery("select birthday, to_unix_timestamp(birthday) from users")
    }

    it("unixTimestamp with TimestampType") {
      testQuery("select created, unix_timestamp(created) from reviews")
    }

    it("unixTimestamp with DateType") {
      testQuery("select birthday, unix_timestamp(birthday) from users")
    }

    it("fromUnixTime") {
      // cast is needed because in MemSQL 6.8 FROM_UNIXTIME query returns a result with microseconds
      testQuery("select id, cast(from_unixtime(id) as timestamp) from movies")
    }

    // MemSQL and Spark differ on how they do last day calculations, so we ignore
    // them in some of these tests

    it("timeAdd") {
      for (interval <- intervals) {
        println(s"testing timeAdd with interval $interval")
        testQuery(s"""
            | select created, created + interval $interval
            | from reviews
            | where date(created) != last_day(created)
            |""".stripMargin)
      }
    }

    it("timeSub") {
      for (interval <- intervals) {
        println(s"testing timeSub with interval $interval")
        testQuery(s"""
            | select created, created - interval $interval
            | from reviews
            | where date(created) != last_day(created)
            |""".stripMargin)
      }
    }

    it("addMonths") {
      val numMonthsList = List(0, 1, 2, 12, 13, 200, -1, -2, -12, -13, -200)
      for (numMonths <- numMonthsList) {
        println(s"testing addMonths with $numMonths months")
        testQuery(s"""
             | select created, add_months(created, $numMonths)
             | from reviews
             | where date(created) != last_day(created)
             |""".stripMargin)
      }
    }

    it("nextDay") {
      for ((dayOfWeek, i) <- com.memsql.spark.ExpressionGen.DAYS_OF_WEEK_OFFSET_MAP) {
        println(s"testing nextDay with $dayOfWeek")
        testQuery(s"""
             | select created, next_day(created, '$dayOfWeek')
             | from reviews
             |""".stripMargin)
      }
    }

    it("nextDay with invalid day name") {
      testQuery(s"""
           | select created, next_day(created, 'invalid_day')
           | from reviews
           |""".stripMargin)
    }

    it("dateDiff") {
      testSingleReadQuery(
        """
          | select birthday, created, DateDiff(birthday, created), DateDiff(created, birthday)
          | from users inner join reviews on users.id = reviews.user_id
          | """.stripMargin)
    }

    it("dateDiff for equal dates") {
      testQuery("""
          | select created, DateDiff(created, created)
          | from reviews
          | """.stripMargin)
    }

    // Spark doesn't support explicit time intervals like `+/-hh:mm`

    val timeZones = List(
      "US/Mountain",
      "Asia/Seoul",
      "UTC",
      "EST",
      "Etc/GMT-6"
    )

    it("fromUTCTimestamp") {
      for (timeZone <- timeZones) {
        println(s"testing fromUTCTimestamp with timezone $timeZone")
        testQuery(s"select from_utc_timestamp(created, '$timeZone') from reviews")
      }
    }

    it("toUTCTimestamp") {
      for (timeZone <- timeZones) {
        println(s"testing toUTCTimestamp with timezone $timeZone")
        testQuery(s"select to_utc_timestamp(created, '$timeZone') from reviews")
      }
    }

    // TruncTimestamp is called as date_trunc() in Spark
    it("truncTimestamp") {
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

    // TruncDate is called as trunc()
    it("truncDate") {
      val dateParts = List("YEAR", "YYYY", "YY", "MON", "MONTH", "MM")
      for (datePart <- dateParts) {
        println(s"testing truncDate with datepart $datePart")
        testQuery(s"select trunc(created, '$datePart') from reviews")
      }
    }

    it("monthsBetween") {
      for (interval <- intervals) {
        println(s"testing monthsBetween with interval $interval")
        testQuery(
          s"select months_between(created, created + interval $interval) from reviews"
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

    it(s"extract") {
      for (periods <- periodsList) {
        for (period <- periods) {
          testQuery(s"SELECT extract($period FROM birthday) as extract_period from users")
          testQuery(s"SELECT extract($period FROM created) as extract_period from reviews")
        }
      }
    }

    it(s"datePart") {
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
        "SELECT make_timestamp(1000, user_id, user_id, user_id, user_id, user_id) FROM reviews")
    }
  }

  describe("partial pushdown") {
    it("ignores spark UDFs") {
      spark.udf.register("myUpper", (s: String) => s.toUpperCase)
      testQuery("select myUpper(first_name), id from users where id in (10,11,12)",
                expectPartialPushdown = true)
    }

    it("join with pure-jdbc relation") {
      testSingleReadQuery(
        """
        | select users.id, concat(first(users.first_name), " ", first(users.last_name)) as full_name
        | from users
        | inner join testdb_jdbc.reviews on users.id = reviews.user_id
        | group by users.id
        | """.stripMargin,
        expectPartialPushdown = true
      )
    }
  }

  describe("like") {
    it("no escape char") {
      testQuery("select first_name like last_name, last_name like first_name escape from users")
    }

    it("with escape char") {
      testQuery(
        "select first_name like last_name escape '^', last_name like first_name escape '^' from users")
    }

    it("with escape char equal to '/'") {
      testQuery(
        "select first_name like last_name escape '/', last_name like first_name escape '/' from users")
    }
  }
}
