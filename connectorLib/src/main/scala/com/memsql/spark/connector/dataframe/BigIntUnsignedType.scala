// scalastyle:off equals.hash.code

package com.memsql.spark.connector.dataframe

import com.google.common.primitives.UnsignedLong
import org.apache.spark.sql.types._


@SQLUserDefinedType(udt = classOf[BigIntUnsignedType])
class BigIntUnsignedValue(val signedLongValue: Long) extends Serializable with MemSQLCustomType {
  val value = BigDecimal(UnsignedLong.fromLongBits(signedLongValue).toString)

  override def toString: String = value.toString

  override def equals(that: Any): Boolean = that match {
    case x: BigIntUnsignedValue => value == x.value
    case _ => super.equals(that)
  }
}

object BigIntUnsignedValue {
  def fromString(str: String): BigIntUnsignedValue = {
    new BigIntUnsignedValue(UnsignedLong.valueOf(str).longValue)
  }
}


/**
 * Spark SQL [[org.apache.spark.sql.types.UserDefinedType]] for MemSQL's `BIGINT UNSIGNED` column type.
 */
class BigIntUnsignedType private() extends UserDefinedType[BigIntUnsignedValue] {
  override def sqlType: DataType = LongType

  override def serialize(obj: Any): Long = {
    obj match {
      case x: BigIntUnsignedValue => x.signedLongValue
      case x: BigDecimal          => x.toLong
      case x: String              => x.toLong
      case x: Long                => x
    }
  }

  override def deserialize(datum: Any): BigIntUnsignedValue = {
    datum match {
      case x: String => BigIntUnsignedValue.fromString(x)
      case x: Long   => new BigIntUnsignedValue(x)
    }
  }

  override def userClass: Class[BigIntUnsignedValue] = classOf[BigIntUnsignedValue]

  override def asNullable: BigIntUnsignedType = this

  override def typeName: String = "bigint unsigned"
}

case object BigIntUnsignedType extends BigIntUnsignedType
