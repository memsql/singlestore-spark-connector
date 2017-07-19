// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark.connector.sql

import org.scalatest.FlatSpec
import com.memsql.spark.connector.util.JDBCImplicits._
import com.memsql.spark.connector._
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType}

/**
  * Test that MemSQL spark connector can read MemSQL tables with type unsigned bigint
  * by converting the numbers (max value 2^64-1) to a DecimalType(20,0).
  * Test that it can read MemSQL tables with type unsigned int by converting the numbers to a LongType.
  * Also checks that saving the DataFrame back to the existing table works.
  */
class UnsignedTypesSpec extends FlatSpec with SharedMemSQLContext{
  override def beforeAll(): Unit = {
    super.beforeAll()
    this.withConnection(conn => {
      conn.withStatement(stmt => {
        stmt.execute("CREATE TABLE datatypes_bigint(id INT, b BIGINT UNSIGNED)")

        stmt.execute("""INSERT INTO datatypes_bigint(id, b) VALUES
          (1, 18446744073709551615),(2, 492839482),(3, 389853)""")

        stmt.execute("CREATE TABLE datatypes_int(id INT, b INT UNSIGNED)")

        stmt.execute("""INSERT INTO datatypes_int(id, b) VALUES
          (1, 2147483647),(2, 499482),(3, 3853)""")
      })
    })
  }

  "Reading a DataFrame from MemSQL" should "convert unsigned bigints to DecimalType" in {
    val df = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".datatypes_bigint"), "disablePartitionPushdown" -> "true"))
      .load()

    val fields = df.schema.fields
    assert(fields(0).dataType == IntegerType)
    assert(fields(1).dataType == DecimalType(20,0))

    val data = df.collect().map(x => x.getDecimal(1)).sortWith((x,y) =>
      x.compareTo(y) match {
        case 1 => true
        case _ => false
      })

    assert(data.length == 3)
    assert(data(0).compareTo(BigDecimal("18446744073709551615").bigDecimal) == 0)
    assert(data(1).compareTo(BigDecimal("492839482").bigDecimal) == 0)
    assert(data(2).compareTo(BigDecimal("389853").bigDecimal) == 0)

    df.saveToMemSQL(dbName, "datatypes_bigint")

    val df2 = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".datatypes_bigint")))
      .load()

    val data2 = df2.collect().map(x => x.getDecimal(1)).sortWith((x,y) =>
      x.compareTo(y) match {
        case 1 => true
        case _ => false
      })
    assert(data2.length == 6)
    assert(data2(0).compareTo(BigDecimal("18446744073709551615").bigDecimal) == 0)
    assert(data2(1).compareTo(BigDecimal("18446744073709551615").bigDecimal) == 0)

    assert(data2(2).compareTo(BigDecimal("492839482").bigDecimal) == 0)
    assert(data2(3).compareTo(BigDecimal("492839482").bigDecimal) == 0)

    assert(data2(4).compareTo(BigDecimal("389853").bigDecimal) == 0)
    assert(data2(5).compareTo(BigDecimal("389853").bigDecimal)  == 0)
  }

  it should "convert unsigned ints to LongType" in {
    val df = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".datatypes_int"), "disablePartitionPushdown" -> "true"))
      .load()

    val fields = df.schema.fields

    assert(fields(0).dataType == IntegerType)
    assert(fields(1).dataType == LongType)

    val data = df.collect().map(x => x.getLong(1)).sortWith(_ < _)

    assert(data.length == 3)
    assert(data(0) == 3853)
    assert(data(1) == 499482)
    assert(data(2) == 2147483647)

    df.saveToMemSQL(dbName, "datatypes_int")

    val df2 = ss
      .read
      .format("com.memsql.spark.connector")
      .options(Map( "path" -> (dbName + ".datatypes_int")))
      .load()

    val data2 = df2.collect().map(x => x.getLong(1)).sortWith(_ < _)

    assert(data2.length == 6)
    assert(data2(0) == 3853)
    assert(data2(1) == 3853)

    assert(data2(2) == 499482)
    assert(data2(3) == 499482)

    assert(data2(4) == 2147483647)
    assert(data2(5) == 2147483647)
  }
}
