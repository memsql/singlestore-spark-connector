// scalastyle:off magic.number regex

package org.apache.spark.sql.memsql

import com.memsql.spark.connector._
import com.memsql.spark.connector.dataframe.{BigIntUnsignedType, BigIntUnsignedValue}
import com.memsql.spark.pushdown.MemSQLPushdownStrategy
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.memsql.test.SharedMemSQLContext
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

class BugfixSpec extends FlatSpec with SharedMemSQLContext with Matchers {

  def recreateSimpleTable: Unit = {
    sqlExec("DROP TABLE IF EXISTS foo")
    sqlExec("CREATE TABLE foo (a INT, SHARD())")
    for (i <- Range(0, 10)) {
      sqlExec("INSERT INTO foo (a) VALUES (?)", i)
    }
  }

  def getPhysicalPlan(query: String): Seq[SparkPlan] = {
    val df = msc.sql(query)
    val plan = df.queryExecution.optimizedPlan
    val strategy = new MemSQLPushdownStrategy(sc)
    strategy(plan)
  }

  "getQuerySchema" should "lock out duplicate columns" in {
    recreateSimpleTable
    intercept[UnsupportedOperationException] {
      msc.getMemSQLCluster.getQuerySchema("select a, a from foo")
    }
  }

  "PushdownStrategy" should "support duplicate columns projected from two Join queries" in {
    recreateSimpleTable
    val join = msc.sql("select * from foo x inner join foo y on x.a = y.a order by x.a")
    val expected = Range(0, 10).map(i => Row(i, i)).toArray
    join.collect should equal (expected)
  }

  "PushdownStrategy" should "support plans with empty aggregates" in {
    recreateSimpleTable
    val plan = getPhysicalPlan("SELECT COUNT(*) FROM (SELECT COUNT(*) FROM foo) x")
    plan should not be (Nil)
  }

  "PushdownStrategy" should "support plans with limits and orders" in {
    recreateSimpleTable
    val plan = getPhysicalPlan("SELECT * FROM foo order by a limit 5")
    plan should not be (Nil)
  }

  "PushdownStrategy" should "support plans with just orders" in {
    recreateSimpleTable
    val plan = getPhysicalPlan("SELECT * FROM foo order by a")
    plan should not be (Nil)
  }

  "BigIntUnsignedType" should "handle the full [0, 2^64) range" in {
    val stringsToTest = Seq(
      "0",
      "1",
      "9223372036854775807",   // 2^63 - 1
      "9223372036854775808",   // 2^63
      "18446744073709551615"   // 2^64 - 1
    )

    val stringsToFail = Seq(
      "18446744073709551616",  // 2^64
      "-1"
    )

    stringsToTest.foreach(numStr => {
      val bigInt = BigIntUnsignedValue.fromString(numStr)
      val s = BigIntUnsignedType.serialize(bigInt)
      val d = BigIntUnsignedType.deserialize(s)

      d.toString shouldBe numStr
    })

    stringsToTest.foreach(numStr => {
      val d = BigIntUnsignedType.deserialize(numStr)

      d.toString shouldBe numStr
    })

    stringsToFail.foreach(numStr => {
      a[NumberFormatException] should be thrownBy BigIntUnsignedValue.fromString(numStr)
    })

    val bigInt = BigIntUnsignedValue.fromString("9223372036854775807")
    val biggerInt = BigIntUnsignedValue.fromString("9223372036854775808")

    bigInt.value.longValue should be > biggerInt.value.longValue
    bigInt.value should be < biggerInt.value

    withStatement(stmt => {
      stmt.execute("CREATE TABLE bigint_table (id INT PRIMARY KEY, a BIGINT UNSIGNED)")
    })

    val rows = Range(0, stringsToTest.length).map { idx =>
      Row(idx, BigDecimal(stringsToTest(idx)))
    }
    val rdd = sc.parallelize(rows)

    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("a", BigIntUnsignedType)
      )
    )
    val actualSchema = msc.table("bigint_table").schema
    schema("id").toString shouldBe actualSchema("id").toString()
    schema("a").toString shouldBe actualSchema("a").toString()

    val df1 = msc.createDataFrame(rdd, schema)
    df1.saveToMemSQL("bigint_table2")
    val df2 = msc.table("bigint_table2")

    val df1_data = df1.sort("id").select("a")
    val df2_data = df2.sort("id").select("a")

    val numRows = stringsToTest.length
    df1_data.showString(numRows) shouldBe df2_data.showString(numRows)
  }
}
