package com.memsql.spark.connector.dataframe

import org.apache.spark.sql.types._


@SQLUserDefinedType(udt = classOf[BigIntUnsignedType])
class BigIntUnsignedValue(val value: Long) extends Serializable with MemSQLCustomType {
  override def toString: String = value.toString
}

/**
 * Spark SQL [[org.apache.spark.sql.types.UserDefinedType]] for MemSQL's `BIGINT UNSIGNED` column type.
 */
class BigIntUnsignedType private() extends UserDefinedType[BigIntUnsignedValue] {
  override def sqlType: DataType = LongType

  override def serialize(obj: Any): Long = {
    obj match {
      case x: BigIntUnsignedValue => x.value
      case x: String       => x.toLong
      case x: Long         => x
    }
  }

  override def deserialize(datum: Any): BigIntUnsignedValue = {
    datum match {
      case x: String => new BigIntUnsignedValue(x.toLong)
      case x: Long => new BigIntUnsignedValue(x)
    }
  }

  override def userClass: Class[BigIntUnsignedValue] = classOf[BigIntUnsignedValue]

  override def asNullable: BigIntUnsignedType = this

  override def typeName: String = "bigint unsigned"
}

case object BigIntUnsignedType extends BigIntUnsignedType
