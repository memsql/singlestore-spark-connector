// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark.connector.sql

import java.math.BigDecimal
import java.sql.{Date, Timestamp}

import com.memsql.spark.connector._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.scalatest.FlatSpec

case class MemSQLType(columnType: String, maybeColumnDefinition: Option[String] = None) {
  def columnName: String = s"t_$columnType"
  def columnDefinition: String = s"$columnName ${maybeColumnDefinition.getOrElse(columnType)}"
}

case class TypeMapping(sparkType: DataType, memsqlType: MemSQLType, values: Seq[Any])

class TestTypesSpec extends FlatSpec with SharedMemSQLContext{
  def types: Seq[TypeMapping] = Seq(
    TypeMapping(ShortType, MemSQLType("TINYINT"), Seq(0.toShort, 16.toShort, null)),
    TypeMapping(ShortType, MemSQLType("SMALLINT"), Seq(0.toShort, 64.toShort, null)),

    TypeMapping(IntegerType, MemSQLType("MEDIUMINT"), Seq(0, 256, null)),
    TypeMapping(IntegerType, MemSQLType("INT"), Seq(0, 1024, null)),

    TypeMapping(LongType, MemSQLType("BIGINT"), Seq(0L, 2048L, null)),

    TypeMapping(DecimalType(38, 30), MemSQLType("DECIMAL", Some("DECIMAL(38, 30)")),
      Seq(new BigDecimal(0.0), new BigDecimal(1 / 3.0), null)),

    // FLOAT, REAL and DOUBLE "NULL" behavior is also covered in NullValueSpec
    TypeMapping(FloatType, MemSQLType("FLOAT"), Seq(0.0f, 1.5f, null)),

    TypeMapping(DoubleType, MemSQLType("REAL"), Seq(0.0, 1.6, null)),
    TypeMapping(DoubleType, MemSQLType("DOUBLE"), Seq(0.0, 1.9, null)),

    TypeMapping(StringType, MemSQLType("CHAR"), Seq("", "a", null)),
    TypeMapping(StringType, MemSQLType("VARCHAR", Some("VARCHAR(255)")), Seq("", "abc", null)),
    TypeMapping(StringType, MemSQLType("TINYTEXT"), Seq("", "def", null)),
    TypeMapping(StringType, MemSQLType("MEDIUMTEXT"), Seq("", "ghi", null)),
    TypeMapping(StringType, MemSQLType("LONGTEXT"), Seq("", "jkl", null)),
    TypeMapping(StringType, MemSQLType("TEXT"), Seq("", "mno", null)),
    TypeMapping(StringType, MemSQLType("ENUM", Some("ENUM('a', 'b', 'c')")), Seq("", "a", null)),
    TypeMapping(StringType, MemSQLType("SET", Some("SET('d', 'e', 'f')")), Seq("", "d", null)),

    TypeMapping(BinaryType, MemSQLType("BIT"),
      Seq(Array(0, 0, 0, 0, 0, 0, 0, 0).map(_.toByte), Array(0, 0, 0, 0, 0, 0, 0, 1).map(_.toByte), null)),
    TypeMapping(BinaryType, MemSQLType("BINARY", Some("BINARY(8)")),
      Seq(Array(0, 0, 0, 0, 0, 0, 0, 0).map(_.toByte), Array(0, 0, 0, 0, 0, 0, 0, 1).map(_.toByte), null)),
    TypeMapping(BinaryType, MemSQLType("VARBINARY", Some("VARBINARY(8)")), Seq(Array(0.toByte), Array(2.toByte), null)),
    TypeMapping(BinaryType, MemSQLType("TINYBLOB"), Seq(Array(0.toByte), Array(4.toByte), null)),
    TypeMapping(BinaryType, MemSQLType("MEDIUMBLOB"), Seq(Array(0.toByte), Array(8.toByte), null)),
    TypeMapping(BinaryType, MemSQLType("LONGBLOB"), Seq(Array(0.toByte), Array(16.toByte), null)),
    TypeMapping(BinaryType, MemSQLType("BLOB"), Seq(Array(0.toByte), Array(32.toByte), null)),

    TypeMapping(DateType, MemSQLType("DATE"), Seq(new Date(0), new Date(200), null)),
    TypeMapping(DateType, MemSQLType("YEAR"), Seq(Date.valueOf("1970-1-1"), Date.valueOf("1999-1-1"), null)),
    TypeMapping(StringType, MemSQLType("TIME"), Seq("12:00:00", "06:43:23", null)),
    //TODO: TIME types shouldn't be longs because MemSQL turns 64000L to 6:40:00

    // TIMESTAMP columns will turn NULL inputs into the current time, unless explicitly created as "TIMESTAMP NULL"
    TypeMapping(TimestampType, MemSQLType("TIMESTAMP", Some("TIMESTAMP NULL")),
      Seq(new Timestamp(0), new Timestamp(1449615940000L), null)),
    TypeMapping(TimestampType, MemSQLType("DATETIME"),
      Seq(new Timestamp(0), new Timestamp(1449615941000L), null))
  )

  "TestTypesSpec" should "do something with types" in {
    println("Creating all_types table")
    withStatement { stmt =>
      stmt.execute("DROP TABLE IF EXISTS all_types")
      stmt.execute(s"""
        CREATE TABLE all_types (
          id INT PRIMARY KEY AUTO_INCREMENT,
          ${types.map(_.memsqlType.columnDefinition).mkString(", ")}
        )
      """)
    }

    println("Inserting values into table")
    val rows = types.head.values.indices.map { idx =>
      val id = idx.toLong + 1
      Row.fromSeq(Seq(id) ++ types.map(_.values(idx)))
    }
    val rdd = sc.parallelize(rows)

    val schema = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".all_types")))
      .load()
      .schema

    val memsqlDF = ss.createDataFrame(rdd, schema)
    memsqlDF.saveToMemSQL("all_types")

    println("Testing round trip values")

    val all_types_df =  ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".all_types")))
      .load()

    assert(TestUtils.equalDFs(memsqlDF, all_types_df))

    println("Testing data returned from dataframe")
    val sparkDF = ss.createDataFrame(memsqlDF.rdd, memsqlDF.schema)
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
              if(typeMapping.memsqlType == MemSQLType("TIME")){
                // TODO: TIME types are currently not working properly
                // (see TODO above)
                // So the Time type would fail the null dataframe check
                // because the hardcoded data must be 0L at this point
                // in time
                Nil
              } else {
                val zeroValue = typeMapping.values.head
                Seq(
                  df.filter(df(column) > zeroValue),
                  df.filter(df(column) > zeroValue && df("id") > 0)
                )
              }
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
              if(typeMapping.memsqlType == MemSQLType("TIME")){
                // TODO: TIME types are currently not working properly
                // (see TODO above)
                // So the Time type would fail the null dataframe check
                // because the hardcoded data must be 0L at this point
                // in time
                Nil
              } else {
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
          }

          basicQueries ++ filterQueries ++ groupQueries ++ complexQueries
        }
      }
    })
  }
}
