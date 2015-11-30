// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.types._

object TestData {
  case class MemSQLType(sqlType: String, sampleValues: Seq[String]) {
    def name: String = "val_" + sqlType.replace("(", "_").replace(")", "").replace(",", "_")
  }

  val memsqlTypes: Seq[MemSQLType] = Seq(
    MemSQLType("int", Seq("1", "2", "3")),
    MemSQLType("bigint", Seq("4", "5", "6")),
    MemSQLType("tinyint", Seq("7", "8", "9")),
    MemSQLType("text", Seq("a", "b", "c")),
    MemSQLType("blob", Seq("e", "f", "g")),
    MemSQLType("char(1)", Seq("a", "b", "c")),
    MemSQLType("varchar(100)", Seq("do", "rae", "me")),
    MemSQLType("varbinary(100)", Seq("one", "two", "three")),
    MemSQLType("decimal(5,1)", Seq("1.1", "2.2", "3.3")),
    MemSQLType("double", Seq("4.4", "5.5", "6.6")),
    MemSQLType("float", Seq("7.7", "8.8", "9.9")),
    MemSQLType("datetime", Seq("1990-08-23 01:01:01.0", "1990-08-23 01:01:02.0", "1990-08-23 01:01:03.0")),
    MemSQLType("timestamp", Seq("1990-08-23 01:01:04.0", "1990-08-23 01:01:05.0", "1990-08-23 01:01:06.0")),
    MemSQLType("date", Seq("1990-08-23", "1990-09-23", "1990-10-23"))
  )

  val sparkSQLTypes = Seq(
    (IntegerType, Seq(1, 2, 3)),
    (LongType, Seq(4L, 5L, 6L)),
    (DoubleType, Seq(7.8, 9.1, 1.2)),
    (FloatType, Seq(2.8f, 3.1f, 4.2f)),
    (ShortType, Seq(7.toShort, 8.toShort, 9.toShort)),
    (ByteType, Seq(10.toByte, 11.toByte, 12.toByte)),
    (BooleanType, Seq(true, false, false)),
    (StringType, Seq("hi", "there", "buddy")),
    (BinaryType, Seq("how".map(_.toByte).toSeq, "are".map(_.toByte).toSeq, "you".map(_.toByte).toSeq)),
    //java.sql time structures expect the year minus 1900
    (TimestampType, Seq(new Timestamp(90, 8, 23, 1, 1, 4, 0), new Timestamp(90, 8, 23, 1, 1, 5, 0), new Timestamp(90, 8, 23, 1, 1, 6, 0))),
    (DateType, Seq(new Date(90, 8, 23), new Date(90, 9, 23), new Date(90, 10, 23))))
}
