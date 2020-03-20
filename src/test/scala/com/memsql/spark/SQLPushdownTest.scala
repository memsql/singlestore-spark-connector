package com.memsql.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.prop.TableDrivenPropertyChecks

class SQLPushdownTest
    extends IntegrationSuiteBase
    with TableDrivenPropertyChecks
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  override def beforeAll() = {
    super.beforeEach() // we want to run beforeEach to set up a spark session

    executeQuery("create database if not exists pushdown")
    writeTable("pushdown.users", spark.read.json("src/test/resources/data/users.json"))
    writeTable("pushdown.movies", spark.read.json("src/test/resources/data/movies.json"))
    writeTable("pushdown.reviews", spark.read.json("src/test/resources/data/reviews.json"))

    writeTable("pushdown.users_sample",
               spark.read
                 .format("memsql")
                 .load("pushdown.users")
                 .sample(0.5)
                 .limit(10))
  }

  override def beforeEach() = {
    super.beforeEach()

    spark.sql("create database pushdown")
    spark.sql("create database pushdown_jdbc")

    def makeTables(sourceTable: String) = {
      spark.sql(
        s"create table pushdown.${sourceTable} using memsql options ('dbtable'='pushdown.${sourceTable}')")
      spark.sql(s"create table pushdown_jdbc.${sourceTable} using jdbc options (${jdbcOptionsSQL(
        s"pushdown.${sourceTable}")})")
    }

    makeTables("users")
    makeTables("users_sample")
    makeTables("movies")
    makeTables("reviews")
  }

  def testQuery(q: String,
                alreadyOrdered: Boolean = false,
                expectPartialPushdown: Boolean = false,
                expectSingleRead: Boolean = false): Unit = {
    spark.sql("use pushdown_jdbc")
    val jdbcDF = spark.sql(q)
    // verify that the jdbc DF works first
    jdbcDF.collect()

    spark.sql("use pushdown")
    val memsqlDF = spark.sql(q)
    if (!continuousIntegration) { memsqlDF.show(4) }

    if (!expectSingleRead) {
      assert(memsqlDF.rdd.getNumPartitions > 1)
    }

    if (!expectPartialPushdown) {
      memsqlDF.queryExecution.optimizedPlan match {
        case SQLGen.Relation(_) => true
        case _                  => fail(s"Failed to pushdown entire query")
      }
    }

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
    it("negative int literal") { testSingleReadQuery("select -1 from users") }

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
        | inner join pushdown_jdbc.reviews on users.id = reviews.user_id
        | group by users.id
        | """.stripMargin,
        expectPartialPushdown = true
      )
    }
  }

}
