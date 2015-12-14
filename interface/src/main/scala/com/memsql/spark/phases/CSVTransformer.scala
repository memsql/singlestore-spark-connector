package com.memsql.spark.phases

import com.memsql.spark.connector.dataframe.BigIntUnsignedType
import com.memsql.spark.etl.api.{StringTransformer, PhaseConfig}
import com.memsql.spark.etl.utils.{PhaseLogger, SimpleJsonSchema}
import org.apache.commons.csv._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.unsafe.types.UTF8String
import spray.json.JsValue

import scala.collection.JavaConversions._

class CSVTransformerException(message: String) extends Exception(message)

case class CSVTransformerConfig(
  delimiter: Option[Char],
  escape: Option[String],
  quote: Option[Char],
  null_string: Option[String],
  columns: JsValue) extends PhaseConfig

class CSVTransformer extends StringTransformer {
  override def transform(sqlContext: SQLContext, rdd: RDD[String], transformConfig: PhaseConfig, logger: PhaseLogger): DataFrame = {
    val config = transformConfig.asInstanceOf[CSVTransformerConfig]

    val csvFormat = getCSVFormat(config)
    val nullString = config.null_string
    val columns = SimpleJsonSchema.parseColumnDefs(config.columns)
    val schema = SimpleJsonSchema.columnsToStruct(columns)

    val parsedRDD = rdd.flatMap(parseCSVLines(_, csvFormat))

    val nulledRDD = nullString match {
      case Some(nullS) => parsedRDD.map { line =>
        line.map { value =>
          if (value.trim() == nullS) null else value
        }
      }
      case None => parsedRDD
    }

    val rowRDD = nulledRDD.map(x => {
      // For each row, remove the values where their corresponding column
      // definition has skip = true. Check the length of the row to make sure
      // that it has the correct number of values both before and after
      // filtering columns.
      if (x.size != columns.size) {
        throw new CSVTransformerException(s"Row with values $x has length ${x.size} but there are ${columns.size} columns defined")
      }
      val values = x
        .zip(columns)
        .filter { case (value, column) => !column.skip.getOrElse(false) }
        .map {
          case (null, _) => null
          case (value, column) => {
            column.column_type match {
              case Some(ShortType) => value.toShort
              case Some(IntegerType) => value.toInt
              case Some(LongType) => value.toLong
              case Some(BigIntUnsignedType) => value.toLong
              case Some(FloatType) => value.toFloat
              case Some(DoubleType) => value.toDouble
              case Some(BooleanType) => value.toBoolean
              case Some(TimestampType) => {
                DateTimeUtils.stringToTimestamp(UTF8String.fromString(value)) match {
                  case None => null
                  case Some(timestamp) => DateTimeUtils.toJavaTimestamp(timestamp)
                }
              }
              case Some(ByteType) => value.toByte
              case Some(BinaryType) => value.map(_.toByte).toArray

              // None, StringType, JsonType, GeographyType, GeographyPointType
              case _ => value
            }
          }
        }
      Row.fromSeq(values)
    })
    sqlContext.createDataFrame(rowRDD, schema)
  }

  private def getCSVFormat(config: CSVTransformerConfig): CSVFormat = {
    val format = CSVFormat.newFormat(config.delimiter.getOrElse(','))
      .withIgnoreSurroundingSpaces()
      .withIgnoreEmptyLines(false)
      .withRecordSeparator('\n')
      .withQuote(config.quote.getOrElse('"'))

    config.escape match {
      case None => format.withEscape('\\')
      case Some("") => format
      case Some(x) if x.size == 1 => format.withEscape(x.head)
      case _ => throw new CSVTransformerException("Escape is not a single character")
    }
  }

  // TODO: support non-standard line delimiters
  private def parseCSVLines(s: String, format: CSVFormat): Iterable[List[String]] = {
    CSVParser.parse(s, format).map(record => record.toList)
  }
}
