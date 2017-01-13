// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark.connector.sql

import org.scalatest.FlatSpec
import com.memsql.spark.connector.util.JDBCImplicits._

/**
  * Test that Spark filters applied to MemSQL dataframes return correct results
  */
class FilterCorrectnessSpec extends FlatSpec with SharedMemSQLContext{
  override def beforeAll(): Unit = {
    super.beforeAll()
    this.withConnection(conn => {
      conn.withStatement(stmt => {
        stmt.execute("""
          CREATE TABLE filtercorrectness
          (id INT PRIMARY KEY, data VARCHAR(200), key(data))
        """)

        val insertValues = Range(0, 40)
          .map(i => s"""($i, 'test_data_${"%02d".format(i)}')""")
          .mkString(",")

        val nullValues = Range(40, 50)
          .map(i => s"""($i, NULL)""")
          .mkString(",")

        stmt.execute("INSERT INTO filtercorrectness VALUES" + insertValues + "," + nullValues)
      })
    })
  }

  "A MemSQL-sourced dataframe" should "correctly apply Spark's Equals filter" in {
    val spark = ss
    import spark.implicits._

    val equalsFilterDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".filtercorrectness")))
      .load()
      .filter($"id" === 2)

    val ids = equalsFilterDataframe.collect().map(x => x.getInt(0)).sortWith(_ < _)

    assert(ids.length === 1)
    assert(ids(0) === 2)
  }

  it should "correctly apply Spark's LessThan filter" in {
    val spark = ss
    import spark.implicits._

    val lessThanDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".filtercorrectness")))
      .load()
      .filter($"id" < 4)

    val ids = lessThanDataframe.collect().map(x => x.getInt(0)).sortWith(_ < _)

    assert(ids.length === 4)

    assert(ids(0) === 0)
    assert(ids(1) === 1)
    assert(ids(2) === 2)
    assert(ids(3) === 3)
  }

  it should "correctly apply Spark's GreaterThan filter" in {
    val spark = ss
    import spark.implicits._

    val greaterThanDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".filtercorrectness")))
      .load()
      .filter($"id" > 47)

    val ids = greaterThanDataframe.collect().map(x => x.getInt(0)).sortWith(_ < _)

    assert(ids.length == 2)

    assert(ids(0) === 48)
    assert(ids(1) === 49)
  }

  it should "correctly apply Spark's LessThanOrEqual filter" in {
    val spark = ss
    import spark.implicits._

    val lessThanOrEqualDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".filtercorrectness")))
      .load()
      .filter($"id" <= 4)

    val ids = lessThanOrEqualDataframe.collect().map(x => x.getInt(0)).sortWith(_ < _)

    assert(ids.length == 5)

    assert(ids(0) === 0)
    assert(ids(1) === 1)
    assert(ids(2) === 2)
    assert(ids(3) === 3)
    assert(ids(4) === 4)
  }

  it should "correctly apply Spark's GreaterThanOrEqual filter" in {
    val spark = ss
    import spark.implicits._

    val greaterThanOrEqualDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".filtercorrectness")))
      .load()
      .filter($"id" >= 47)

    val ids = greaterThanOrEqualDataframe.collect().map(x => x.getInt(0)).sortWith(_ < _)

    assert(ids.length == 3)

    assert(ids(0) === 47)
    assert(ids(1) === 48)
    assert(ids(2) === 49)
  }

  it should "correctly apply Spark's IsNotNull filter" in {
    val spark = ss
    import spark.implicits._

    val isNotNullDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".filtercorrectness")))
      .load()
      .filter($"data".isNotNull)

    val data = isNotNullDataframe.collect()

    assert(data.length == 40)
  }

  it should "correctly apply Spark's IsNull filter" in {
    val spark = ss
    import spark.implicits._

    val isNullDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".filtercorrectness")))
      .load()
      .filter($"data".isNull)

    val data = isNullDataframe.collect()

    assert(data.length == 10)
  }

  it should "correctly apply Spark's StringStartsWith filter" in {
    val spark = ss
    import spark.implicits._

    val stringStartsWithDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".filtercorrectness")))
      .load()
      .filter($"data".startsWith("test_data_1"))

      val data = stringStartsWithDataframe.collect().map(x => x.getString(1)).sortWith(_ < _)

    assert(data.length == 10) // test_data_10 through test_data_19

    assert(data(0).equals("test_data_10"))
    assert(data(1).equals("test_data_11"))
    assert(data(2).equals("test_data_12"))
  }

  it should "correctly apply Spark's StringEndsWith filter" in {
    val spark = ss
    import spark.implicits._

    val stringEndsWithDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".filtercorrectness")))
      .load()
      .filter($"data".endsWith("0"))

    val data = stringEndsWithDataframe.collect().map(x => x.getString(1)).sortWith(_ < _)

    assert(data.length == 4) // test_data_00, 10, 20, 30

    assert(data(0).equals("test_data_00"))
    assert(data(1).equals("test_data_10"))
    assert(data(2).equals("test_data_20"))
    assert(data(3).equals("test_data_30"))
  }

  it should "correctly apply Spark's StringContains filter" in {
    val spark = ss
    import spark.implicits._

    val stringContainsDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".filtercorrectness")))
      .load()
      .filter($"data".contains("5"))

    val data = stringContainsDataframe.collect().map(x => x.getString(1)).sortWith(_ < _)

    assert(data.length == 4) // test_data_05, 15, 25, 35

    assert(data(0).equals("test_data_05"))
    assert(data(1).equals("test_data_15"))
    assert(data(2).equals("test_data_25"))
  }

  it should "correctly apply Spark's In filter" in {
    val spark = ss
    import spark.implicits._

    val items = List("test_data_00", "test_data_01")

    val inDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".filtercorrectness")))
      .load()
      .filter($"data".isin(items:_*)) // according to documentation, isin takes a vararg, not a list

    val data = inDataframe.collect().map(x => x.getString(1)).sortWith(_ < _)

    assert(data.length == 2)

    assert(data(0).equals("test_data_00"))
    assert(data(1).equals("test_data_01"))
  }

  it should "correctly apply Spark's Not filter" in {
    val spark = ss
    import spark.implicits._
    import org.apache.spark.sql.functions.not

    val notDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".filtercorrectness")))
      .load()
      .filter(not($"id" < 40))

    val ids = notDataframe.collect().map(x => x.getInt(0)).sortWith(_ < _)

    assert(ids.length == 10) //test_data_40 - 49

    assert(ids(0) === 40)
    assert(ids(1) === 41)
    assert(ids(2) === 42)
    assert(ids(3) === 43)
    assert(ids(9) === 49)
  }

  it should "correctly apply Spark's Or filter" in {
    val spark = ss
    import spark.implicits._

    val orDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".filtercorrectness")))
      .load()
      .filter(($"id" === 2) || ($"id" === 4))

    val ids = orDataframe.collect().map(x => x.getInt(0)).sortWith(_ < _)

    assert(ids.length == 2)

    assert(ids(0) === 2)
    assert(ids(1) === 4)
  }

  it should "correctly apply Spark's And filter" in {
    val spark = ss
    import spark.implicits._

    val andDataframe = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".filtercorrectness")))
      .load()
      .filter(($"id" > 10) && ($"id" < 14))

    val ids = andDataframe.collect().map(x => x.getInt(0)).sortWith(_ < _)

    assert(ids.length == 3) // ids 11, 12, 13

    assert(ids(0) === 11)
    assert(ids(1) === 12)
    assert(ids(2) === 13)
  }
}
