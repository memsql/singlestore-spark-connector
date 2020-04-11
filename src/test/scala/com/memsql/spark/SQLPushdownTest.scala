package com.memsql.spark

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
    spark.sql("create database testdb_jdbc")

    def makeTables(sourceTable: String) = {
      spark.sql(
        s"create table testdb.${sourceTable} using memsql options ('dbtable'='testdb.${sourceTable}')")
      spark.sql(s"create table testdb_jdbc.${sourceTable} using jdbc options (${jdbcOptionsSQL(
        s"testdb.${sourceTable}")})")
    }

    makeTables("users")
    makeTables("users_sample")
    makeTables("movies")
    makeTables("reviews")
  }

  def testQuery(q: String,
                alreadyOrdered: Boolean = false,
                expectPartialPushdown: Boolean = false,
                expectSingleRead: Boolean = false,
                expectEmpty: Boolean = false): Unit = {
    spark.sql("use testdb_jdbc")
    val jdbcDF = spark.sql(q)
    // verify that the jdbc DF works first
    jdbcDF.collect()

    spark.sql("use testdb")
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
      assertApproximateDataFrameEquality(memsqlDF, jdbcDF, 0.1, orderedComparison = alreadyOrdered)
    } catch {
      case e: Throwable => {
        if (continuousIntegration) { println(memsqlDF.explain(true)) }
        throw e
      }
    }
  }

  def testOrderedQuery(q: String, expectPartialPushdown: Boolean = false): Unit = {
    // order by in MemSQL requires single read
    testQuery(q, alreadyOrdered = true, expectPartialPushdown, expectSingleRead = true)
  }

  def testSingleReadQuery(q: String,
                          alreadyOrdered: Boolean = false,
                          expectPartialPushdown: Boolean = false): Unit = {
    testQuery(q, alreadyOrdered, expectPartialPushdown, expectSingleRead = true)
  }

  describe("sanity test the tables") {
    it("select all users") { testQuery("select * from users") }
    it("select all users (sampled)") { testQuery("select * from users_sample") }
    it("select all movies") { testQuery("select * from movies") }
    it("select all reviews") { testQuery("select * from reviews") }
  }

  describe("math expressions") {
    it("sinh") { testQuery("select sinh(rating) from reviews") }
    it("cosh") { testQuery("select cosh(rating) from reviews") }
    it("tanh") { testQuery("select tanh(rating) from reviews") }
  }

  describe("datatypes") {
    it("null literal") { testSingleReadQuery("select null from users") }
    it("int literal") { testQuery("select 1 from users") }
    it("bool literal") { testQuery("select true from users") }
    it("float, bool literal") { testSingleReadQuery("select 1.2 as x, true from users") }

    // due to a bug in our dataframe comparison library we need to alias the column 4.9 to x...
    // this is because when the library asks spark for a column called "4.9", spark thinks the
    // library wants the table 4 and column 9.
    it("float literal") { testSingleReadQuery("select 4.9 as x from movies") }

    it("negative float literal") { testSingleReadQuery("select -24.345 as x from movies") }
    it("negative int literal") { testQuery("select -1 from users") }

    it("int") { testQuery("select id from users") }
    it("smallint") { testQuery("select age from users") }
    it("date") { testQuery("select birthday from users") }
    it("datetime") { testQuery("select created from reviews") }
    it("bool") { testQuery("select owns_house from users") }
    it("float") { testQuery("select critic_rating from movies") }
    it("text") { testQuery("select first_name from users") }
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
    it("count distinct") { testSingleReadQuery("select count(distinct first_name) from users") }
    it("first") { testSingleReadQuery("select first(first_name) from users group by id") }
    it("last") { testSingleReadQuery("select last(first_name) from users group by id") }
    it("floor(avg(age))") { testSingleReadQuery("select floor(avg(age)) from users") }
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
      testSingleReadQuery("""
          |  select users.id, round(avg(rating), 2), count(*) as num_reviews
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

}
