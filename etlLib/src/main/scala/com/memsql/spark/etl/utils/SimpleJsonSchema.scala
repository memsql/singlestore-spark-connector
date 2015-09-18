package com.memsql.spark.etl.utils

import com.memsql.spark.connector.dataframe.JsonType
import org.apache.spark.sql.types.StructField
import spray.json._
import org.apache.spark.sql.types._

case class ColumnDefinition(
  name: Option[String] = None,
  column_type: Option[DataType] = None,
  skip: Option[Boolean] = None
)

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

  implicit val columnDefinitionFormat = jsonFormat3(ColumnDefinition)

  object columnsFormat extends RootJsonReader[List[ColumnDefinition]] {
    def read(value: JsValue): List[ColumnDefinition] = value match {
      case JsArray(columns) => columns.map(columnDefinitionFormat.read).toList
      case _ => throw new DeserializationException("JSON array expected.")
    }

    def write(l: List[ColumnDefinition]) = throw new Exception("Writing Columns is not supported")
  }
}

object SimpleJsonSchema {
  def parseColumnDefs(rawColumns: JsValue): List[ColumnDefinition] = {
    SimpleJsonSchemaProtocol.columnsFormat.read(rawColumns)
  }

  def columnsToStruct(columns: List[ColumnDefinition]): StructType = {
    val fields = columns
      .filterNot(_.skip.getOrElse(false))
      .map(c => {
        if (c.name.isEmpty) {
          throw new DeserializationException("Columns must have a name")
        }
        val columnType = c.column_type.getOrElse(StringType)
        StructField(c.name.get, columnType, true)
      })
    return StructType(fields)
  }
}
