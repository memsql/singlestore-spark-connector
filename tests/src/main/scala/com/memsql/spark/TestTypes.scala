// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark

import java.sql.{Date, Timestamp}
import java.math.BigDecimal
import com.memsql.spark.pushdown.MemSQLPushdownStrategy
import com.memsql.spark.connector._
import org.apache.spark.sql.memsql.test.TestUtils
import org.apache.spark.sql.{Row, Column, DataFrame}
import org.apache.spark.sql.memsql.MemSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, Logging}

case class MemSQLType(columnType: String, maybeColumnDefinition: Option[String] = None) {
  def columnName: String = s"t_$columnType"
  def columnDefinition: String = s"$columnName ${maybeColumnDefinition.getOrElse(columnType)}"
}

case class TypeMapping(sparkType: DataType, memsqlType: MemSQLType, values: Seq[Any])

object TestTypes {
  def main(args: Array[String]): Unit = new TestTypes
}

class TestTypes extends TestApp with Logging {
  def types: Seq[TypeMapping] = Seq(
    TypeMapping(ShortType, MemSQLType("TINYINT"), Seq(0.toShort, 16.toShort)),
    TypeMapping(ShortType, MemSQLType("SMALLINT"), Seq(0.toShort, 64.toShort)),

    TypeMapping(IntegerType, MemSQLType("MEDIUMINT"), Seq(0, 256)),
    TypeMapping(IntegerType, MemSQLType("INT"), Seq(0, 1024)),

    TypeMapping(LongType, MemSQLType("BIGINT"), Seq(0L, 2048L)),

    TypeMapping(DecimalType(38, 30), MemSQLType("DECIMAL", Some("DECIMAL(38, 30)")),
      Seq(new BigDecimal(0.0), new BigDecimal(1 / 3.0))),

    TypeMapping(FloatType, MemSQLType("FLOAT"), Seq(0.0f, 1.5f)),

    TypeMapping(DoubleType, MemSQLType("REAL"), Seq(0.0, 1.6)),
    TypeMapping(DoubleType, MemSQLType("DOUBLE"), Seq(0.0, 1.9)),

    TypeMapping(StringType, MemSQLType("CHAR"), Seq("", "a")),
    TypeMapping(StringType, MemSQLType("VARCHAR", Some("VARCHAR(255)")), Seq("", "abc")),
    TypeMapping(StringType, MemSQLType("TINYTEXT"), Seq("", "def")),
    TypeMapping(StringType, MemSQLType("MEDIUMTEXT"), Seq("", "ghi")),
    TypeMapping(StringType, MemSQLType("LONGTEXT"), Seq("", "jkl")),
    TypeMapping(StringType, MemSQLType("TEXT"), Seq("", "mno")),
    TypeMapping(StringType, MemSQLType("ENUM", Some("ENUM('a', 'b', 'c')")), Seq("", "a")),
    TypeMapping(StringType, MemSQLType("SET", Some("SET('d', 'e', 'f')")), Seq("", "d")),

    TypeMapping(BinaryType, MemSQLType("BIT"),
      Seq(Array(0, 0, 0, 0, 0, 0, 0, 0).map(_.toByte), Array(0, 0, 0, 0, 0, 0, 0, 1).map(_.toByte))),
    TypeMapping(BinaryType, MemSQLType("BINARY", Some("BINARY(8)")),
      Seq(Array(0, 0, 0, 0, 0, 0, 0, 0).map(_.toByte), Array(0, 0, 0, 0, 0, 0, 0, 1).map(_.toByte))),
    TypeMapping(BinaryType, MemSQLType("VARBINARY", Some("VARBINARY(8)")), Seq(Array(0.toByte), Array(2.toByte))),
    TypeMapping(BinaryType, MemSQLType("TINYBLOB"), Seq(Array(0.toByte), Array(4.toByte))),
    TypeMapping(BinaryType, MemSQLType("MEDIUMBLOB"), Seq(Array(0.toByte), Array(8.toByte))),
    TypeMapping(BinaryType, MemSQLType("LONGBLOB"), Seq(Array(0.toByte), Array(16.toByte))),
    TypeMapping(BinaryType, MemSQLType("BLOB"), Seq(Array(0.toByte), Array(32.toByte))),

    TypeMapping(DateType, MemSQLType("DATE"), Seq(new Date(0), new Date(200))),
    TypeMapping(DateType, MemSQLType("YEAR"), Seq(new Date(0), new Date(300))),
    TypeMapping(LongType, MemSQLType("TIME"), Seq(0L, 64000L)),

    TypeMapping(TimestampType, MemSQLType("TIMESTAMP"), Seq(new Timestamp(0), new Timestamp(1449615940000L))),
    TypeMapping(TimestampType, MemSQLType("DATETIME"), Seq(new Timestamp(0), new Timestamp(1449615941000L)))
  )

  def runTest(sc: SparkContext, msc: MemSQLContext): Unit = {
    val mscNoPushdown = new MemSQLContext(sc)
    MemSQLPushdownStrategy.unpatchSQLContext(mscNoPushdown)

    logInfo("Creating all_types table")
    withStatement { stmt =>
      stmt.execute("DROP TABLE IF EXISTS all_types")
      stmt.execute(s"""
        CREATE TABLE all_types (
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
    val schema = msc.table("all_types").schema
    val memsqlDF = msc.createDataFrame(rdd, schema)
    memsqlDF.saveToMemSQL("all_types")

    logInfo("Testing round trip values")
    assert(TestUtils.equalDFs(memsqlDF, msc.table("all_types")))

    logInfo("Testing data returned from dataframe")
    val sparkDF = mscNoPushdown.createDataFrame(memsqlDF.rdd, memsqlDF.schema)
    val allDFs = (sparkDF, memsqlDF)

    val aggregateFunctions: Seq[String => Column] = Seq(avg, count, max, min, sum)

    TestUtils.runQueries[DataFrame](allDFs, {
      case (df: DataFrame) => {
        types.flatMap { typeMapping =>
          val column = typeMapping.memsqlType.columnName

          val basicQueries = Seq(
            df,
            df.select(column),
            df.select(df(column).as("test")),
            df.select(df(column), df(column)),
            df.orderBy(df(column).desc)
          )

          val filterQueries = typeMapping.sparkType match {
            //TODO currently DateType cannot be filtered
            case DateType => Nil
            case _ => {
              val zeroValue = typeMapping.values.head
              Seq(
                df.filter(df(column) > zeroValue),
                df.filter(df(column) > zeroValue && df("id") > 0)
              )
            }
          }

          val groupQueries = typeMapping.sparkType match {
            // SparkSQL does not support aggregating BinaryType
            case BinaryType => Nil
            // MySQL doesn't have the same semantics as SparkSQL for aggregating dates
            case DateType => {
              Seq(
                df.groupBy(df(column)).count()
              )
            }
            case _ => {
              Seq(
                df.groupBy(df(column)).count()
              ) ++ aggregateFunctions.map { fn =>
                df.groupBy(df("id")).agg(fn(column))
              }
            }
          }

          val complexQueries = typeMapping.sparkType match {
            // SparkSQL does not support aggregating BinaryType
            case BinaryType => Nil
            //TODO currently DateType cannot be filtered
            case DateType => Nil
            case _ => {
              aggregateFunctions.map { fn =>
                val zeroValue = typeMapping.values.head
                df.select(df(column).as("foo"), df("id"))
                  .filter(col("foo") > zeroValue)
                  .groupBy("id")
                  .agg(fn("foo") as "bar")
                  .orderBy("bar")
              }
            }
          }

          basicQueries ++ filterQueries ++ groupQueries ++ complexQueries
        }
      }
    })
  }
}
