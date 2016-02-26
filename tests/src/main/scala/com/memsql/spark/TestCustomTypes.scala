// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark

import com.memsql.spark.connector._
import com.memsql.spark.connector.dataframe._
import com.memsql.spark.pushdown.MemSQLPushdownStrategy
import org.apache.spark.sql.memsql.MemSQLContext
import org.apache.spark.sql.memsql.test.TestUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{Logging, SparkContext}

object TestCustomTypes {
  def main(args: Array[String]): Unit = new TestCustomTypes
}

class TestCustomTypes extends TestApp with Logging {
  def types: Seq[TypeMapping] = Seq(
    TypeMapping(BigIntUnsignedType, MemSQLType("BIGINT_UNSIGNED", Some("BIGINT UNSIGNED")),
      Seq(BigIntUnsignedValue.fromString("0"),
          BigIntUnsignedValue.fromString("9223372036854775808"))),

    TypeMapping(DatetimeType, MemSQLType("DATETIME", Some("DATETIME")),
      Seq(DatetimeValue.fromString("2014-02-02T12:25:35"),
          DatetimeValue.fromString("2014-02-02 12:25:35"),
          DatetimeValue.fromString("0"),
          DatetimeValue.fromString("2014-02-02"))),

    TypeMapping(GeographyPointType, MemSQLType("GEOGRAPHYPOINT"),
      Seq(Array(80, 79, 73, 78, 84, 40, 48, 46, 48, 48, 48, 48, 48, 48,
        48, 52, 32, 48, 46, 48, 48, 48, 48, 48, 48, 48, 52, 41).map(_.toByte),
      Array(80, 79, 73, 78, 84, 40, 49, 55, 46, 51, 48, 48, 48, 48,
        48, 48, 50, 32, 52, 50, 46, 57, 48, 48, 48, 48, 48, 48, 50, 41).map(_.toByte))),

    TypeMapping(GeographyType, MemSQLType("GEOGRAPHY"),
      Seq("POINT(0.00000004 0.00000004)", "POINT(0.00000004 0.00000004)")),

    TypeMapping(JsonType, MemSQLType("JSON"),
      Seq("{\"foo\":\"bar\"}", "{\"foo\":\"baz\"}"))
  )

  def runTest(sc: SparkContext, msc: MemSQLContext): Unit = {
    val mscNoPushdown = new MemSQLContext(sc)
    MemSQLPushdownStrategy.unpatchSQLContext(mscNoPushdown)

    logInfo("Creating custom_types table")

    withStatement { stmt =>
      stmt.execute("DROP TABLE IF EXISTS custom_types")
      stmt.execute(s"""
        CREATE TABLE custom_types (
          id INT PRIMARY KEY AUTO_INCREMENT,
          ${types.map(_.memsqlType.columnDefinition).mkString(", ")}
        )
      """)
    }

    logInfo("Inserting values into table")
    val rows = types.head.values.indices.map { idx =>
      val id = idx.toLong + 1
      Row.fromSeq(Seq(id) ++ types.map(_.values(idx)))
    }
    val rdd = sc.parallelize(rows)
    val schema = msc.table("custom_types").schema
    val memsqlDF = msc.createDataFrame(rdd, schema)

    memsqlDF.saveToMemSQL("custom_types")

    logInfo("Testing round trip values")
    assert(TestUtils.equalDFs(memsqlDF, msc.table("custom_types")))

    logInfo("Testing data returned from dataframe")
    val sparkDF = mscNoPushdown.createDataFrame(memsqlDF.rdd, memsqlDF.schema)
    val allDFs = (sparkDF, memsqlDF)

    TestUtils.runQueries[DataFrame](allDFs, {
      case (df: DataFrame) => {
        types.flatMap { typeMapping =>
          val column = typeMapping.memsqlType.columnName

          val basicQueries = Seq(
            df,
            df.select(column),
            df.select(df(column).as("test")),
            df.select(df(column), df(column))
          )

          basicQueries
        }
      }
    })
  }
}
