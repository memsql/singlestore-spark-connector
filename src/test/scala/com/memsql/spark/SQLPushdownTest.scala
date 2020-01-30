package com.memsql.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.prop.TableDrivenPropertyChecks

class SQLPushdownTest
    extends IntegrationSuiteBase
    with TableDrivenPropertyChecks
    with BeforeAndAfterEach {

  override def beforeEach() = {
    super.beforeEach()

    executeQuery("create database if not exists pushdown")
    spark.sql("create database if not exists pushdown")
    spark.sql("create database if not exists pushdown_jdbc")

    executeQuery("drop table if exists pushdown.users")
    executeQuery("""
      create table pushdown.users (
        id int primary key,
        first_name text not null,
        last_name text not null,
        email text not null,
        owns_house bool not null,
        favorite_color text ,
        age smallint,
        birthday date
      )
    """)
    val usersDF = spark.read.json("src/test/resources/data/users.json")
    writeTable("pushdown.users", usersDF, SaveMode.Append)
    executeQuery("analyze table pushdown.users")

    spark.sql("drop table if exists pushdown.users")
    spark.sql("create table pushdown.users using memsql options ('dbtable'='pushdown.users')")

    spark.sql("drop table if exists pushdown_jdbc.users")
    spark.sql(
      s"create table pushdown_jdbc.users using jdbc options (${jdbcOptionsSQL("pushdown.users")})")

    executeQuery("drop table if exists pushdown.movies")
    executeQuery("""
      create table pushdown.movies (
        id int primary key,
        title text not null,
        genre text not null,
        critic_review text,
        critic_rating float
      )
    """)
    val moviesDF = spark.read.json("src/test/resources/data/movies.json")
    writeTable("pushdown.movies", moviesDF, SaveMode.Append)
    executeQuery("analyze table pushdown.movies")

    spark.sql("drop table if exists pushdown.movies")
    spark.sql("create table pushdown.movies using memsql options ('dbtable'='pushdown.movies')")

    spark.sql("drop table if exists pushdown_jdbc.movies")
    spark.sql(
      s"create table pushdown_jdbc.movies using jdbc options (${jdbcOptionsSQL("pushdown.movies")})")

    executeQuery("drop table if exists pushdown.reviews")
    executeQuery("""
      create table pushdown.reviews (
        user_id int not null,
        movie_id int not null,
        rating float not null,
        review text not null,
        created datetime not null
      )
    """)
    val reviewsDF = spark.read.json("src/test/resources/data/reviews.json")
    writeTable("pushdown.reviews", reviewsDF, SaveMode.Append)
    executeQuery("analyze table pushdown.reviews")

    spark.sql("drop table if exists pushdown.reviews")
    spark.sql("create table pushdown.reviews using memsql options ('dbtable'='pushdown.reviews')")

    spark.sql("drop table if exists pushdown_jdbc.reviews")
    spark.sql(
      s"create table pushdown_jdbc.reviews using jdbc options (${jdbcOptionsSQL("pushdown.reviews")})")
  }

  def testQuery(q: String, o: Boolean = false) = {
    spark.sql("use pushdown_jdbc")
    val jdbcDF = spark.sql(q)
    // verify that the jdbc DF works first
    jdbcDF.collect()

    spark.sql("use pushdown")
    val memsqlDF = spark.sql(q)
    try {
      assertApproximateDataFrameEquality(memsqlDF, jdbcDF, 0.1, orderedComparison = o)
    } catch {
      case e: Throwable => {
        if (continuousIntegration) { println(memsqlDF.explain(true)) }
        throw e
      }
    }
  }

  def testOrderedQuery(q: String) = {
    testQuery(q, true)
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
      testQuery("select * from users, reviews where users.id = reviews.user_id")
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
      testQuery("select first_name, first_name from users")
    }
    it("select same column twice from table with aliases") {
      testOrderedQuery("select first_name as a, first_name as a from users order by id")
    }
    it("select same alias twice (different column) from table") {
      testOrderedQuery("select first_name as a, last_name as a from users order by id")
    }
    it("select same column twice in subquery") {
      testQuery("select * from (select first_name, first_name from users) as x")
    }
    it("select same column twice from subquery with aliases") {
      testOrderedQuery(
        "select * from (select first_name as a, first_name as a from users order by id) as x")
    }
  }

  describe("partial pushdown") {
    it("ignores spark UDFs") {
      spark.udf.register("myUpper", (s: String) => s.toUpperCase)
      testQuery("select myUpper(first_name), id from users where id in (10,11,12)")
    }

    it("supports a join between memsql and jdbc dataframe with some basic pushdowns") {
      val actualDF = spark
        .sql("""
               | select users.id, concat(first(users.first_name), " ", first(users.last_name)) as full_name, count(*) as count
               | from pushdown.users
               | inner join pushdown_jdbc.reviews on users.id = reviews.user_id
               | where users.id = 10
               | group by users.id
               | """.stripMargin)

      val expectedDF =
        spark.createDF(List((10, "Giffer Makeswell", 4L)),
                       List(
                         ("id", IntegerType, true),
                         ("full_name", StringType, true),
                         ("count", LongType, false)
                       ))

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

  }

}
