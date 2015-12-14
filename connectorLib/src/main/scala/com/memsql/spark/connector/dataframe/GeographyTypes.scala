package com.memsql.spark.connector.dataframe

import org.apache.spark.sql.types._


@SQLUserDefinedType(udt = classOf[GeographyType])
class GeographyValue(val value: String) extends Serializable with MemSQLCustomType {
  override def toString: String = value

  override def equals(o: Any): Boolean = {
    o match {
      case other: GeographyValue => value == other.value
      case _ => false
    }
  }
}

/**
 * Spark SQL [[org.apache.spark.sql.types.UserDefinedType]] for MemSQL's `GEOGRAPHY` column type.
 */
class GeographyType private() extends UserDefinedType[GeographyValue] {
  override def sqlType: DataType = StringType

  override def serialize(obj: Any): String = {
    obj match {
      case x: GeographyValue => x.value
      case x: String         => x
    }
  }

  override def deserialize(datum: Any): GeographyValue = {
    datum match {
      case x: String => new GeographyValue(x)
    }
  }

  override def userClass: Class[GeographyValue] = classOf[GeographyValue]

  override def asNullable: GeographyType = this

  override def typeName: String = "geography"
}

case object GeographyType extends GeographyType

@SQLUserDefinedType(udt = classOf[GeographyPointType])
class GeographyPointValue(val value: String) extends Serializable with MemSQLCustomType {
  override def toString: String = value

  override def equals(o: Any): Boolean = {
    o match {
      case other: GeographyPointValue => value == other.value
      case _ => false
    }
  }
}

/**
 * Spark SQL [[org.apache.spark.sql.types.UserDefinedType]] for MemSQL's `GEOGRAPHYPOINT` column type.
 */
class GeographyPointType private() extends UserDefinedType[GeographyPointValue] {
  override def sqlType: DataType = StringType

  override def serialize(obj: Any): String = {
    obj match {
      case x: GeographyPointValue => x.value
      case x: String         => x
    }
  }

  override def deserialize(datum: Any): GeographyPointValue = {
    datum match {
      case x: String => new GeographyPointValue(x)
    }
  }

  override def userClass: Class[GeographyPointValue] = classOf[GeographyPointValue]

  override def asNullable: GeographyPointType = this

  override def typeName: String = "geographypoint"
}

case object GeographyPointType extends GeographyPointType
