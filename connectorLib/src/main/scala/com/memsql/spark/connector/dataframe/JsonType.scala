package com.memsql.spark.connector.dataframe

import org.apache.spark.sql.types._


@SQLUserDefinedType(udt = classOf[JsonType])
class JsonValue(val value: String) extends Serializable {
  override def toString: String = value
}

/**
 * Spark SQL [[org.apache.spark.sql.types.UserDefinedType]] for MemSQL's `JSON` column type.
 */
class JsonType private() extends UserDefinedType[JsonValue] {
  override def sqlType: DataType = StringType

  override def serialize(obj: Any): String = {
    obj match {
      case x: JsonValue => x.value
      case x: String    => x
    }
  }

  override def deserialize(datum: Any): JsonValue = {
    datum match {
      case x: String => new JsonValue(x)
    }
  }

  override def userClass: Class[JsonValue] = classOf[JsonValue]

  override def asNullable: JsonType = this

  override def typeName: String = "json"
}

case object JsonType extends JsonType
