package com.singlestore.spark

import com.singlestore.spark.SQLGen.SinglestoreVersion
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class SQLPushdownTestAiq extends IntegrationSuiteBase with BeforeAndAfterEach with BeforeAndAfterAll {

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

  describe("Null Expressions") {
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
  }

  describe("Arithmetic Expressions") {
    describe("ToNumber") {
      val (f, s) = ("ToNumber", "to_number")

      it(s"$f works with non-nullable columns") {
        testQuery(
          s"""
            |select
            | $s(cast(user_id as string), '999') as ${f.toLowerCase}1,
            | $s(cast(rating as string), '9.9') as ${f.toLowerCase}2
            |from reviews
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it(s"$f works with nullable column") {
        testQuery(
          s"select $s(cast(critic_rating as string), '9.9') as ${f.toLowerCase} from movies"
        )
      }
      it(s"$f with partial pushdown because of udf") {
        testQuery(
          s"""
            |select
            | $s(stringIdentity(user_id), '999') as ${f.toLowerCase}1,
            | $s(stringIdentity(rating), '9.9') as ${f.toLowerCase}2
            |from reviews
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("bitwise Expressions") {
    val functionsGroup = Seq(
      ("BitAndAgg", "bit_and", "user_id", "reviews"),
      ("BitOrAgg", "bit_or", "age", "users"),
      ("BitXorAgg", "bit_xor", "user_id", "reviews")
    )

    for ((f, n, c, t) <- functionsGroup) {
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
    val functions = Seq("acosh", "bin", "cot", "hex", "sec", "unhex").sorted

    for (f <- functions) {
      describe(f) {
        f match {
          case "bin" | "hex" =>
            it(s"$f works with long column") {
              testQuery(s"select user_id, $f(user_id) as $f from reviews")
            }
          case "unhex" =>
            it(s"$f works with string column") {
              testQuery(s"select hex(review) as review_hex, $f(hex(review)) as $f from reviews")
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
             |${if (f == "hex") s"user_id, $f(longIdentity(user_id)) as ${f}0" else s"rating, $f(floatIdentity(rating)) as ${f}0"},
             |${if (f == "unhex") s"$f(stringIdentity(review)) as ${f}1" else "stringIdentity(review) as review"}
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

  describe("Aggregate Expressions") {
    val functionsGroup = Seq(
      "skewness",
      "kurtosis",
      "var_pop", "var_samp",
      "stddev_samp", "stddev_pop",
      "avg", "min", "max", "sum",
      "approx_count_distinct"
    ).sorted

    for (f <- functionsGroup) {
      describe(f) {
        it(s"$f works with group by clause and long non-nullable column") {
          testSingleReadForOldS2(
            s"select $f(id) as ${f.toLowerCase.replace("_", "")} from movies group by genre",
            SinglestoreVersion(7, 6, 0),
            expectSameResult = if (f == "approx_count_distinct") false else true
          )
        }
        it(s"$f works with group by clause and float nullable column") {
          testSingleReadForOldS2(
            s"""
              |select $f(critic_rating) as ${f.toLowerCase.replace("_", "")}
              |from movies
              |group by genre
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            SinglestoreVersion(7, 6, 0),
            expectSameResult = if (f == "approx_count_distinct") false else true
          )
        }
      }
    }

    val functionsGroup1 = Seq("first", "last").sorted

    for (f <- functionsGroup1) {
      describe(f.capitalize) {
        ignore(s"09/2024 - ${f.capitalize} works with non-nullable string column") {
          testSingleReadForReadFromLeaves(s"select $f(first_name) as $f from users group by id")
        }
        ignore(s"09/2024 - ${f.capitalize} with partial pushdown because of udf") {
          testSingleReadQuery(
            s"select $f(stringIdentity(first_name)) as $f from users group by id",
            expectPartialPushdown = true
          )
        }
        ignore(s"09/2024 - ${f.capitalize} works with filter") {
          testSingleReadForReadFromLeaves(
            s"select $f(first_name) filter (where age % 2 = 0) as $f from users group by id"
          )
        }
      }
    }

    describe("HyperLogLogPlusPlus") {
      // `approx_count_distinct` is not accurate, so we don't expect
      // the same results between Spark and SingleStore
      val (f, s) = ("HyperLogLogPlusPlus", "approx_count_distinct")

      it (s"$f works with non-nullable int column") {
        testSingleReadForOldS2(
          s"select $s(user_id) as ${f.toLowerCase} from reviews",
          SinglestoreVersion(7, 6, 0),
          expectSameResult = false
        )
      }
      it(s"$f works with non-nullable float column") {
        testSingleReadForOldS2(
          s"select $s(cast(rating as decimal(2, 1))) as ${f.toLowerCase} from reviews",
          SinglestoreVersion(7, 6, 0),
          expectSameResult = false
        )
      }
      it(s"$f works with nullable float column") {
        testSingleReadForOldS2(
          s"select $s(cast(critic_rating as decimal(2, 1))) as ${f.toLowerCase} from movies",
          SinglestoreVersion(7, 6, 0),
          expectSameResult = false
        )
      }
      it(s"$f works with filter") {
        testSingleReadForOldS2(
          s"select $s(age) filter (where age % 2 = 0) as ${f.toLowerCase} from users",
          SinglestoreVersion(7, 6, 0),
          expectSameResult = false
        )
      }
      it(s"$f works with filter for equal range population(std = 0)") {
        testSingleReadForOldS2(
          s"select $s(age) filter (where age = 60) as ${f.toLowerCase} from users",
          SinglestoreVersion(7, 6, 0),
          expectSameResult = false
        )
      }
      it(s"$f with partial pushdown because of udf") {
        testSingleReadQuery(
          s"select $s(longIdentity(user_id)) as ${f.toLowerCase} from reviews",
          expectPartialPushdown = true,
          expectSameResult = false
        )
      }
    }

    describe("ApproximatePercentile") {
      val (f, s) = ("ApproximatePercentile", "percentile_approx")

      it(s"$f works with long non-nullable column and single percentile value") {
        testQuery(
          s"""
            |select
            | $s(id, 0.25) as ${f.toLowerCase}1,
            | $s(id, 0.5) as ${f.toLowerCase}2,
            | $s(id, 0.75d) as ${f.toLowerCase}3,
            | $s(id, 0.0) as ${f.toLowerCase}4,
            | $s(id, 1.0) as ${f.toLowerCase}5,
            | $s(id, 0) as ${f.toLowerCase}6,
            | $s(id, 1) as ${f.toLowerCase}7
            |from users
           """.stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it(s"$f works with long non-nullable column and the first element satisfies small percentages") {
        testQuery(
          s"""
            |select
            | $s(id, 0.01) as ${f.toLowerCase}1,
            | $s(id, 0.1) as ${f.toLowerCase}2,
            | $s(id, 0.11) as ${f.toLowerCase}3
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it(s"$f works with numeric nullable columns") {
        testQuery(
          s"""
            |select
            | $s(critic_rating, 0.25) as ${f.toLowerCase}1,
            | $s(cast(critic_rating as decimal(2,1)), 0.5) as ${f.toLowerCase}2
            |from movies
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }

      val accuracies = Seq(100, 1000, 10000).sorted

      for (accuracy <- accuracies) {
        it(s"$f works with numeric nullable columns and accuracy $accuracy") {
          testQuery(
            s"""
              |select
              | $s(cast(critic_rating as decimal(2,1)), 0.25, $accuracy) as ${f.toLowerCase}1,
              | $s(cast(critic_rating as decimal(2,1)), 0.5, $accuracy) as ${f.toLowerCase}2
              |from movies
              |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
          )
        }
      }

      it(s"$f works with group by") {
        testQuery(
          s"""
            |select
            | id,
            | $s(cast(critic_rating as decimal(2,1)), 0.5) as ${f.toLowerCase}
            |from movies
            |group by id
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      it(s"$f works with window function") {
        testQuery(
          s"""
            |select
            | $s(id, 0.5)
            |    over (
            |      partition by id
            |      order by birthday
            |      rows between unbounded preceding and current row
            |    ) as ${f.toLowerCase}
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      // approx_percentile in singlestore does NOT support
      // arrays in the percentile argument as spark does
      it(s"$f with partial pushdown because of array in the percentile argument") {
        testSingleReadQuery(
          s"""
            |select
            | $s(id, array(0.25, 0.5, 0.75D)) as ${f.toLowerCase}1,
            | $s(id, array(0.01, 0.1, 0.11)) as ${f.toLowerCase}2
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
      // approx_percentile in singlestore does NOT support
      // timestamp or date columns as spark does
      it(s"$f with partial pushdown because of non-numeric columns") {
        testSingleReadQuery(
          s"select $s(birthday, 0.25) as ${f.toLowerCase} from users",
          expectPartialPushdown = true
        )
        testSingleReadQuery(
          s"select $s(created, 0.25) as ${f.toLowerCase} from reviews",
          expectPartialPushdown = true
        )
      }
      it(s"$f with partial pushdown because of udf") {
        testSingleReadQuery(
          s"select $s(longIdentity(id), 0.25) as ${f.toLowerCase} from users",
          expectPartialPushdown = true
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

    describe("NthValue") {
      val (f, s) = ("NthValue", "nth_value")

      val offsetList = Seq(2, 3, 4).sorted

      for (offset <- offsetList) {
        it(s"$f partition order by works with non-nullable columns and offset $offset") {
          testQuery(
            s"""
              |select
              | $s(id, $offset) over (partition by age order by id) as ${f.toLowerCase}
              |from users
              |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
          )
        }
        it(s"$f partition order by works with nullable columns and offset $offset") {
          testQuery(
            s"""
              |select
              | $s(id, $offset) over (partition by genre order by id) as ${f.toLowerCase}
              |from movies
              |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
          )
        }
        it(s"$f with partial pushdown because of udf and offset $offset") {
          testSingleReadQuery(
            s"""
              |select
              | $s(id, $offset)
              |    over (partition by stringIdentity(genre) order by id) as ${f.toLowerCase}
              |from movies
              |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
            expectPartialPushdown = true
          )
        }
      }
    }
  }

  describe("Regular Expressions") {
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
  }

  describe("Datetime Expressions") {
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

    describe("DateFromUnixDate") {
      val (f, s) = ("DateFromUnixDate", "date_from_unix_date")

      it(s"$f works with simple literal") {
        testQuery(s"select $s(1234567) as ${f.toLowerCase} from users")
      }
      it(s"$f works with simple null") {
        testQuery(s"select $s(null) as ${f.toLowerCase} from users")
      }
    }

    // ConvertTimezone SQL support starts Spark 3.4 and over
    // For now, this Expression will be tested through AIQ Date Functions
    describe("ConvertTimezone") {
      val (f, s) = ("ConvertTimezone", "convert_timezone")

      ignore(s"09/2024 - $f works with timestamp non-nullable column") {
        testQuery(
          s"select $s(created, 'UTC', 'America/Los_Angeles') as ${f.toLowerCase} from reviews"
        )
      }
      ignore(s"09/2024 - $f works with timestamp non-nullable column and null in second and/or third argument") {
        testQuery(
          s"""
            |select
            | $s(created, null, 'America/Los_Angeles') as ${f.toLowerCase}1,
            | $s(created, 'UTC', null) as ${f.toLowerCase}2,
            | $s(created, null, null) as ${f.toLowerCase}3
            |from reviews
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      ignore(s"09/2024 - $f works with string non-nullable column") {
        testQuery(
          s"""
            |select
            | $s(cast(created as string), 'UTC', 'America/Los_Angeles') as ${f.toLowerCase}
            |from reviews
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      ignore(
        s"09/2024 - $f works with string non-nullable column and null in second and/or third argument"
      ) {
        testQuery(
          s"""
            |select
            | $s(cast(created as string), null, 'America/Los_Angeles') as ${f.toLowerCase}1,
            | $s(cast(created as string), 'UTC', null) as ${f.toLowerCase}2,
            | $s(cast(created as string), null, null) as ${f.toLowerCase}3
            |from reviews
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      ignore(s"09/2024 - $f works with date non-nullable column") {
        testQuery(
          s"select $s(birthday, 'UTC', 'America/Los_Angeles') as ${f.toLowerCase} from users"
        )
      }
      ignore(s"09/2024 - $f works with date non-nullable column and null in second and/or third argument") {
        testQuery(
          s"""
            |select
            | $s(birthday, null, 'America/Los_Angeles') as ${f.toLowerCase}1,
            | $s(birthday, 'UTC', null) as ${f.toLowerCase}2,
            | $s(birthday, null, null) as ${f.toLowerCase}3
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" ")
        )
      }
      ignore(s"09/2024 - $f with partial pushdown because of udf in the first argument") {
        testQuery(
          s"""
            |select $s(stringIdentity(birthday), null, 'America/Los_Angeles') as ${f.toLowerCase}
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
      ignore(s"09/2024 - $f with partial pushdown because of udf in the second argument") {
        testQuery(
          s"""
            |select $s(birthday, stringIdentity('UTC'), 'America/Los_Angeles') as ${f.toLowerCase}
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
      ignore(s"09/2024 - $f with partial pushdown because of udf in the third argument") {
        testQuery(
          s"""
            |select $s(birthday, 'UTC', stringIdentity('America/Los_Angeles')) as ${f.toLowerCase}
            |from users
            |""".stripMargin.linesIterator.map(_.trim).mkString(" "),
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("String Expressions") {
    val functionsGroup = Seq(
      ("StringTrim", "trim", "both"),
      ("StringTrimLeft", "ltrim", "leading"),
      ("StringTrimRight", "rtrim", "trailing")
    ).sorted

    for ((f, s, d) <- functionsGroup) {
      describe(f) {
        it(s"$f works with non-nullable column") {
          testQuery(s"select id, $s(first_name) as ${f.toLowerCase} from users")
        }

        if(Seq("StringTrim").contains(f)) {
          it(s"$f works with non-nullable column (other syntax)") {
            testQuery(s"select id, b$s(first_name) as ${f.toLowerCase} from users")
          }
        }

        it(s"$f works when trimStr is ' '") {
          testQuery(s"select id, trim($d ' ' from first_name) as ${f.toLowerCase} from users")
        }
        it(s"$f works when trimStr is ' ' (other syntax)") {
          testQuery(s"select id, $s(' ', first_name) as ${f.toLowerCase} from users")
        }
        it(s"$f works when trimStr is not None and not ' '") {
          testQuery(s"select id, trim($d '@' from first_name) as ${f.toLowerCase} from users")
        }
        it(s"$f works when trimStr is not None and not ' ' (other syntax)") {
          testQuery(s"select id, $s('@', first_name) as ${f.toLowerCase} from users")
        }
        it(s"$f with partial pushdown because of udf") {
          testQuery(
            s"select id, $s(stringIdentity(first_name)) as ${f.toLowerCase} from users",
            expectPartialPushdown = true
          )
        }
      }
    }

    describe("Base64") {
      val (f, s) = ("Base64", "base64")

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
      it(s"$f works with nullable string column", ExcludeFromSpark34, ExcludeFromSpark35) {
        testQuery(
          s"""
            |select
            | id,
            | substr($s(critic_review), 1, 76) as ${f.toLowerCase}1,
            | substr($s(critic_review), -1, 76) as ${f.toLowerCase}2
            |from movies
            |""".stripMargin.linesIterator.mkString(" ")
        )
      }
      it(s"$f with partial pushdown because of udf") {
        testQuery(
          s"select id, $s(stringIdentity(critic_review)) as ${f.toLowerCase} from movies",
          expectPartialPushdown = true
        )
      }
    }

    describe("UnBase64") {
      val (f, s) = ("UnBase64", "unbase64")

      it(s"$f works with nullable column", ExcludeFromSpark34, ExcludeFromSpark35) {
        testQuery(s"select id, $s(base64(critic_review)) as ${f.toLowerCase} from movies")
      }
      it(s"$f with partial pushdown because of udf") {
        testQuery(
          s"select id, $s(base64(stringIdentity(critic_review))) as ${f.toLowerCase} from movies",
          expectPartialPushdown = true
        )
      }
    }
  }

  describe("JSON Functions") {
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
