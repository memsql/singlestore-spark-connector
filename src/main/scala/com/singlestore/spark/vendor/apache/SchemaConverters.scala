package com.singlestore.spark.vendor.apache

import org.apache.avro._
import org.apache.spark.sql.types._

/**
  *  NOTE: this converter has been taken from `spark-avro` library,
  *  as this functionality starts with spark 2.4.0 but we support lower versions.
  *  org.apache.spark.sql.avro.SchemaConverters  (2.4.0 version)
  *   Changes:
  *   1. Removed everything except toAvroType function
  */
object SchemaConverters {

  private lazy val nullSchema = Schema.create(Schema.Type.NULL)

  case class SchemaType(dataType: DataType, nullable: Boolean)

  def toAvroType(catalystType: DataType,
                 nullable: Boolean = false,
                 recordName: String = "topLevelRecord",
                 nameSpace: String = ""): Schema = {
    val builder = SchemaBuilder.builder()

    val schema = catalystType match {
      case BooleanType                        => builder.booleanType()
      case ByteType | ShortType | IntegerType => builder.intType()
      case LongType                           => builder.longType()
      case DateType =>
        LogicalTypes.date().addToSchema(builder.intType())
      case TimestampType =>
        LogicalTypes.timestampMicros().addToSchema(builder.longType())

      case FloatType      => builder.floatType()
      case DoubleType     => builder.doubleType()
      case StringType     => builder.stringType()
      case _: DecimalType => builder.stringType()
      case BinaryType     => builder.bytesType()
      case ArrayType(et, containsNull) =>
        builder
          .array()
          .items(toAvroType(et, containsNull, recordName, nameSpace))
      case MapType(StringType, vt, valueContainsNull) =>
        builder
          .map()
          .values(toAvroType(vt, valueContainsNull, recordName, nameSpace))
      case st: StructType =>
        val childNameSpace  = if (nameSpace != "") s"$nameSpace.$recordName" else recordName
        val fieldsAssembler = builder.record(recordName).namespace(nameSpace).fields()
        st.foreach { f =>
          val fieldAvroType =
            toAvroType(f.dataType, f.nullable, f.name, childNameSpace)
          fieldsAssembler.name(f.name).`type`(fieldAvroType).noDefault()
        }
        fieldsAssembler.endRecord()

      // This should never happen.
      case other => throw new IncompatibleSchemaException(s"Unexpected type $other.")
    }
    if (nullable) {
      Schema.createUnion(schema, nullSchema)
    } else {
      schema
    }
  }
}

class IncompatibleSchemaException(msg: String, ex: Throwable = null) extends Exception(msg, ex)
