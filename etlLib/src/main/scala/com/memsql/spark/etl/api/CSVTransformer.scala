package com.memsql.spark.etl.api

import java.util.NoSuchElementException

import org.apache.spark.rdd._
import org.apache.spark.sql._
import com.memsql.spark.etl.utils.{SimpleJsonSchema, PhaseLogger}
import com.memsql.spark.etl.api.configs._
import scala.collection.JavaConversions._
import org.apache.commons.csv._
import spray.json._

case class CSVTransformerConfig(
  delimiter: Option[Char],
  escape: Option[Char],
  quote: Option[Char],
  null_string: Option[String],
  columns: JsValue)

object CSVTransformerProtocol extends DefaultJsonProtocol {
  val csvTransformerConfigFormat = jsonFormat5(CSVTransformerConfig)
}

class CSVTransformer extends SimpleByteArrayTransformer {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], userConfig: UserTransformConfig, logger: PhaseLogger): DataFrame = {
    val config = CSVTransformerProtocol.csvTransformerConfigFormat.read(userConfig.value)
    val csvFormat = getCSVFormat(config)
    val nullString = config.null_string
    val schema = SimpleJsonSchema.jsonSchemaToStruct(config.columns)

    val parsedRDD = rdd.map(byteUtils.bytesToUTF8String)
                       .flatMap(parseCSVLines(_, csvFormat))

    val nulledRDD = nullString match {
      case Some(nullS) => parsedRDD.map(x => x.map(y => if (y.trim() == nullS) None else y))
      case None => parsedRDD
    }

    val rowRDD = nulledRDD.map(x => Row.fromSeq(x))
    return sqlContext.createDataFrame(rowRDD, schema)
  }

  private def getCSVFormat(config: CSVTransformerConfig): CSVFormat = {
    // The MYSQL format is a sensible base format (you need to pick one in CSVFormat). We
    // override most of the options via the config, so the choice is not too significant.
    return CSVFormat.MYSQL
      .withDelimiter(config.delimiter.getOrElse(','))
      .withEscape(config.escape.getOrElse('\\'))
      .withQuote(config.quote.getOrElse('"'))
      .withIgnoreSurroundingSpaces()
  }

  // TODO: support non-standard line delimiters
  private def parseCSVLines(s: String, format: CSVFormat): Iterable[List[String]] = {
    return CSVParser.parse(s, format).map(record => record.toList)
  }
}
