package com.memsql.spark

import java.sql.SQLNonTransientConnectionException

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.SaveMode
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
  }

  override def beforeEach() = {
    super.beforeEach()

    spark.sql("create database pushdown")
    spark.sql("create database pushdown_jdbc")

    spark.sql("create table pushdown.users using memsql options ('dbtable'='pushdown.users')")
    spark.sql(
      s"create table pushdown_jdbc.users using jdbc options (${jdbcOptionsSQL("pushdown.users")})")

    spark.sql("create table pushdown.movies using memsql options ('dbtable'='pushdown.movies')")
    spark.sql(
      s"create table pushdown_jdbc.movies using jdbc options (${jdbcOptionsSQL("pushdown.movies")})")

    spark.sql("create table pushdown.reviews using memsql options ('dbtable'='pushdown.reviews')")
    spark.sql(
      s"create table pushdown_jdbc.reviews using jdbc options (${jdbcOptionsSQL("pushdown.reviews")})")
  }

  def testQuery(q: String, o: Boolean = false, expectPartialPushdown: Boolean = false) = {
    spark.sql("use pushdown_jdbc")
    val jdbcDF = spark.sql(q)
    // verify that the jdbc DF works first
    jdbcDF.collect()

    spark.sql("use pushdown")
    val memsqlDF = spark.sql(q)
    if (!continuousIntegration) { memsqlDF.show(4) }

    if (!expectPartialPushdown) {
      memsqlDF.queryExecution.optimizedPlan match {
        case SQLGen.Relation(_) => true
        case _                  => fail(s"Failed to pushdown entire query")
      }
    }

    try {
      assertApproximateDataFrameEquality(memsqlDF, jdbcDF, 0.1, orderedComparison = o)
    } catch {
      case e: Throwable => {
        if (continuousIntegration) { println(memsqlDF.explain(true)) }
        throw e
      }
    }
  }

  def testOrderedQuery(q: String, expectPartialPushdown: Boolean = false) = {
    testQuery(q, true, expectPartialPushdown)
  }

  describe("sanity test the tables") {
    it("select all users") { testQuery("select * from users") }
    it("select all movies") { testQuery("select * from movies") }
    it("select all reviews") { testQuery("select * from reviews") }
  }

  describe("datatypes") {
    it("null literal") { testQuery("select null from users") }
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
  }

  describe("filter") {
    it("numeric equality") { testQuery("select * from users where id = 1") }
    it("numeric inequality") { testQuery("select * from users where id != 1") }
    it("numeric comparison >") { testQuery("select * from users where id > 500") }
    it("numeric comparison > <") { testQuery("select * from users where id > 500 and id < 550") }
    it("string equality") { testQuery("select * from users where first_name = 'Evan'") }
  }

  describe("aggregations") {
    it("count") { testQuery("select count(*) from users") }
    it("count distinct") { testQuery("select count(distinct first_name) from users") }
    it("first") { testQuery("select first(first_name) from users group by id") }
    it("last") { testQuery("select last(first_name) from users group by id") }
    it("floor(avg(age))") { testQuery("select floor(avg(age)) from users") }
    it("top 3 email domains") {
      testOrderedQuery("""
        |   select domain, count(*) from (
        |     select substring(email, locate('@', email) + 1) as domain
        |     from users
        |   )
        |   group by 1
        |   order by 2 desc, 1 asc
        |   limit 3
        |""".stripMargin)
    }
  }

  describe("window functions") {
    it("rank order by") {
      testQuery("select out as a from (select rank() over (order by first_name) as out from users)")
    }
    it("rank partition order by") {
      testQuery(
        "select rank() over (partition by first_name order by first_name) as out from users")
    }
    it("row_number order by") {
      testQuery("select row_number() over (order by first_name) as out from users")
    }
    it("dense_rank order by") {
      testQuery("select dense_rank() over (order by first_name) as out from users")
    }
    it("lag order by") {
      testQuery("select first_name, lag(first_name) over (order by first_name) as out from users")
    }
    it("lead order by") {
      testQuery("select first_name, lead(first_name) over (order by first_name) as out from users")
    }
    it("ntile(3) order by") {
      testQuery("select first_name, ntile(3) over (order by first_name) as out from users")
    }
    it("percent_rank order by") {
      testQuery("select first_name, percent_rank() over (order by first_name) as out from users")
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
      testQuery("select * from users as a, reviews where a.id = reviews.user_id")
    }
    it("explicit inner join") {
      testQuery("select * from users inner join reviews on users.id = reviews.user_id")
    }
    it("cross join") {
      testQuery("select * from users cross join reviews on users.id = reviews.user_id")
    }
    it("left outer join") {
      testQuery("select * from users left outer join reviews on users.id = reviews.user_id")
    }
    it("right outer join") {
      testQuery("select * from users right outer join reviews on users.id = reviews.user_id")
    }
    it("full outer join") {
      testQuery("select * from users full outer join reviews on users.id = reviews.user_id")
    }
    it("natural join") {
      testQuery(
        "select users.id, rating from users natural join (select user_id as id, rating from reviews)")
    }
    it("complex join") {
      testQuery("""
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
      testQuery(
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
