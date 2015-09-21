package com.memsql.spark.etl.utils

import com.memsql.spark.connector.dataframe.{BigIntUnsignedType, GeographyType, GeographyPointType, JsonType}
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
      case JsString(column) => column.toUpperCase match {
        case "BYTE"            => ByteType
        case "SHORT"           => ShortType
        case "INT"             => IntegerType
        case "INTEGER"         => IntegerType
        case "BIGINT"          => LongType
        case "BIGINT UNSIGNED" => BigIntUnsignedType
        case "FLOAT"           => FloatType
        case "DOUBLE"          => DoubleType
        case "STRING"          => StringType
        case "BINARY"          => BinaryType
        case "BOOL"            => BooleanType
        case "BOOLEAN"         => BooleanType
        case "TIMESTAMP"       => TimestampType
        case "DATE"            => TimestampType
        case "DATETIME"        => TimestampType
        case "JSON"            => JsonType
        case "GEOGRAPHY"       => GeographyType
        case "GEOGRAPHYPOINT"  => GeographyPointType
        case _ => throw new DeserializationException("Unknown type " + column)
      }
      case _ => throw new DeserializationException("ColumnType must be a string")
    }

    def write(d: DataType): JsValue = throw new Exception("Writing ColumnTypes is not supported")
  }

  implicit val columnDefinitionFormat = jsonFormat3(ColumnDefinition)

  object columnsFormat extends RootJsonReader[List[ColumnDefinition]] {
    def read(value: JsValue): List[ColumnDefinition] = value match {
      case JsArray(columns) => columns.map(columnDefinitionFormat.read).toList
      case _ => throw new DeserializationException("JSON array expected.")
    }

    def write(l: List[ColumnDefinition]): JsValue = throw new Exception("Writing Columns is not supported")
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
    StructType(fields)
  }
}
