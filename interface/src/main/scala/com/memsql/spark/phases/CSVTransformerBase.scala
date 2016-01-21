package com.memsql.spark.phases

import com.memsql.spark.etl.api.{StringTransformer}
import com.memsql.spark.etl.utils.{SimpleJsonSchema}
import org.apache.commons.csv._
import org.apache.spark.rdd._

import scala.collection.JavaConversions._

class CSVTransformerException(message: String) extends Exception(message)

abstract class CSVTransformerBase extends StringTransformer {
  def getNulledRDD(rdd: RDD[String], csvFormat: CSVFormat, nullString: Option[String]): RDD[List[String]] = {
    val parsedRDD = rdd.flatMap(parseCSVLines(_, csvFormat))
    nullString match {
      case Some(nullS) => parsedRDD.map { line =>
        line.map { value =>
          if (value == nullS) null else value
        }
      }
      case None => parsedRDD
    }
  }

  def getCSVFormat(delimiter: Option[Char], escape: Option[String], quote: Option[Char]): CSVFormat = {
    val format = CSVFormat.newFormat(delimiter.getOrElse(','))
      .withIgnoreSurroundingSpaces()
      .withIgnoreEmptyLines(false)
      .withRecordSeparator('\n')
      .withQuote(quote.getOrElse('"'))

    escape match {
      case None => format.withEscape('\\')
      case Some("") => format
      case Some(x) if x.size == 1 => format.withEscape(x.head)
      case _ => throw new CSVTransformerException("Escape is not a single character")
    }
  }

  // TODO: support non-standard line delimiters
  def parseCSVLines(s: String, format: CSVFormat): Iterable[List[String]] = {
    CSVParser.parse(s, format).map(record => record.toList)
  }
}
