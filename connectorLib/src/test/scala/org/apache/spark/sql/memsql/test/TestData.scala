// scalastyle:off magic.number file.size.limit regex

package org.apache.spark.sql.memsql.test

import java.sql.{Date, Timestamp}
import java.util.TimeZone

import com.memsql.spark.connector.sql.ColumnDefinition
import org.apache.spark.sql.types._

object TestData {
  case class MemSQLType(sqlType: String, sampleValues: Seq[Any]) {
    val name = "val_" + sqlType.replace("(", "_").replace(")", "").replace(",", "_")
    val columnDefn = ColumnDefinition(name, sqlType)
  }

  val utcOffset = -1 * TimeZone.getDefault.getRawOffset
  val memsqlTypes: Seq[MemSQLType] = Seq(
    MemSQLType("int", Seq(0, 2, 1041241)),
    MemSQLType("bigint", Seq(0L, 5L, 123123123L)),
    MemSQLType("tinyint", Seq(0.toShort, 7.toShort, 40.toShort)),
    MemSQLType("smallint", Seq(0.toShort, 64.toShort, 100.toShort)),
    MemSQLType("text", Seq("aasdfasfasfasdfasdfa", "", "ʕ ᓀ ᴥ ᓂ ʔ")),
    MemSQLType("blob", Seq("e", "f", "g")),
    MemSQLType("bool", Seq(0.toShort, 1.toShort, 1.toShort)),
    MemSQLType("char(1)", Seq("a", "b", "c")),
    MemSQLType("varchar(100)", Seq("do", "rae", "me")),
    MemSQLType("varbinary(100)", Seq("one", "two", "three")),
    MemSQLType("decimal(20,10)", Seq(BigDecimal(3.00033358), BigDecimal(3.442), BigDecimal(121231.12323))),
    MemSQLType("real", Seq(0.5, 2.3, 123.13451)),
    MemSQLType("double", Seq(0.3, 2.7, 234324.2342)),
    MemSQLType("float", Seq(0.5f, 3.4f, 123.1234f)),
    MemSQLType("datetime", Seq(new Timestamp(utcOffset), new Timestamp(1449615940000L), new Timestamp(1049615940000L))),
    MemSQLType("timestamp", Seq(new Timestamp(utcOffset), new Timestamp(1449615940000L), new Timestamp(1049615940000L))),
    MemSQLType("date", Seq(new Date(90, 8, 23), new Date(100, 3, 5), new Date(utcOffset)))
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
