package com.memsql.spark.etl.utils

import com.memsql.spark.connector.dataframe.JsonType
import org.apache.spark.sql.types.StructField
import spray.json._
import org.apache.spark.sql.types._

case class ColumnDefinition(name: String, column_type: DataType)

object SimpleJsonSchemaProtocol extends JsonEnumProtocol {
  implicit object columnTypeFormat extends RootJsonFormat[DataType] {
    def read(value: JsValue): DataType = value match {
      case JsString(column) => column match {
        case "byte"     => ByteType
        case "short"    => ShortType
        case "int"      => IntegerType
        case "integer"  => IntegerType
        case "long"     => LongType
        case "float"    => FloatType
        case "double"   => DoubleType
        case "string"   => StringType
        case "binary"   => BinaryType
        case "bool"     => BooleanType
        case "boolean"  => BooleanType
        case "timestamp"=> TimestampType
        case "date"     => TimestampType
        case "datetime" => TimestampType
        case "json"     => JsonType
        case _ => throw new DeserializationException("Unknown type " + column)
      }
      case _ => throw new DeserializationException("ColumnType must be a string")
    }

    def write(d: DataType) = throw new Exception("Writing ColumnTypes is not supported")
  }

  implicit val columnDefinitionFormat = jsonFormat2(ColumnDefinition)

  object columnsFormat extends RootJsonReader[List[ColumnDefinition]] {
    def read(value: JsValue): List[ColumnDefinition] = value match {
      case JsArray(columns) => columns.map(columnDefinitionFormat.read).toList
      case _ => throw new DeserializationException("JSON array expected.")
    }

    def write(l: List[ColumnDefinition]) = throw new Exception("Writing Columns is not supported")
  }
}

object SimpleJsonSchema {
  def jsonSchemaToStruct(rawColumns: JsValue): StructType = {
    val columns = SimpleJsonSchemaProtocol.columnsFormat.read(rawColumns)
    val fields = columns.map(c => StructField(c.name, c.column_type, true))
    return StructType(fields)
  }
}

