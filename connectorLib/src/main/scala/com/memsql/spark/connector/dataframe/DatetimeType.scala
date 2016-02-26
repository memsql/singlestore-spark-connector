// scalastyle:off equals.hash.code
package com.memsql.spark.connector.dataframe

import java.sql.Timestamp
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.unsafe.types.UTF8String


@SQLUserDefinedType(udt = classOf[DatetimeType])
class DatetimeValue(val value: Timestamp) extends Serializable with MemSQLCustomType {
  override def toString: String = value.toString

  override def equals(that: Any): Boolean = that match {
    case x: DatetimeValue => value.equals(x.value)
    case x: Timestamp => value.equals(x)
    case _ => super.equals(that)
  }
}

object DatetimeValue {
  def fromString(str: String): DatetimeValue = {
    if (str == null) {
      null
    } else {
      val timestamp = DateTimeUtils.toJavaTimestamp(DateTimeUtils.stringToTimestamp(UTF8String.fromString(str)).get)
      new DatetimeValue(timestamp)
    }
  }
}


/**
 * Spark SQL [[org.apache.spark.sql.types.UserDefinedType]] for MemSQL's `DATETIME` column type.
 */
class DatetimeType private() extends UserDefinedType[DatetimeValue] {
  override def sqlType: DataType = TimestampType

  override def serialize(obj: Any): Timestamp = {
    obj match {
      case x: DatetimeValue       => x.value
      case x: String              => DatetimeValue.fromString(x).value
      case x: Timestamp           => x
    }
  }

  override def deserialize(datum: Any): DatetimeValue = {
    datum match {
      case x: String => DatetimeValue.fromString(x)
      case x: Timestamp   => new DatetimeValue(x)
    }
  }

  override def userClass: Class[DatetimeValue] = classOf[DatetimeValue]

  override def asNullable: DatetimeType = this

  override def typeName: String = "datetime"
}

case object DatetimeType extends DatetimeType
