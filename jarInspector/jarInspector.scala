package com.memsql.jar_inspector

import java.io.File
import java.util.Properties
import spray.json._
import org.apache.log4j.PropertyConfigurator

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.reflections.util.{ ConfigurationBuilder, FilterBuilder }
import org.reflections.Reflections
import org.reflections.scanners._

case class JarInfo(target: File=null)
case class InspectionResult(
  success: Boolean,
  extractors: List[String]=List(),
  transformers: List[String]=List(),
  error: Option[String]=None
)

object JsonProto extends DefaultJsonProtocol {
  val resultFormat = jsonFormat4(InspectionResult)
}

object JarInspector {
  val MAX_SUBCLASSES = 1024;
  val VERSION = "0.0.1"

  val EXTRACTOR_ROOT_CLASS = "com.memsql.spark.etl.api.Extractor"
  val TRANSFORMER_ROOT_CLASS = "com.memsql.spark.etl.api.Transformer"

  // We won't include any extractors or transformers that begin with this
  // prefix in the output
  val IGNORE_PREFIX = "com.memsql.spark.etl"

  def getAllSubclasses(subTypesMap: Map[String, List[String]], root_class: String): mutable.ListBuffer[String] = {
    var index = 0
    var result = mutable.ListBuffer[String](root_class)
    while (index < result.length) {
      subTypesMap.get(result(index)).foreach(result.appendAll(_))
      index += 1
      if (index > MAX_SUBCLASSES) {
        throw new Exception(s"Jar file contains more than $MAX_SUBCLASSES implementations of $root_class")
      }
    }
    result.filterNot(_.startsWith(IGNORE_PREFIX))
  }

  def inspectJarFile(jarFile: File): InspectionResult = {
    var config = new ConfigurationBuilder()
      .setUrls(mutable.ListBuffer(jarFile.toURI.toURL))
      .setScanners(new SubTypesScanner(false))

    var reflect = new Reflections(config)

    val subTypesMap = mapAsScalaMap(reflect.getStore.get("SubTypesScanner").asMap)
      .map({ x => (x._1, x._2.toList)}).toMap

    val extractors = getAllSubclasses(subTypesMap, EXTRACTOR_ROOT_CLASS).toList
    val transformers = getAllSubclasses(subTypesMap, TRANSFORMER_ROOT_CLASS).toList

    if (extractors.length == 0 && transformers.length == 0) {
      throw new Exception(s"Jar file does not contain any valid Extractor or Transformer implementations.")
    }

    InspectionResult(
      success = true,
      extractors = getAllSubclasses(subTypesMap, EXTRACTOR_ROOT_CLASS).toList,
      transformers = getAllSubclasses(subTypesMap, TRANSFORMER_ROOT_CLASS).toList
    )
  }

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("log4j.rootCategory", "INFO, console")
    props.setProperty("log4j.appender.console", "org.apache.log4j.varia.NullAppender")
    PropertyConfigurator.configure(props)

    val parser = new scopt.OptionParser[JarInfo]("jar-inspector") {
      override def showUsageOnError = true

      head("jarInspector", JarInspector.VERSION)
      opt[File]("target") required() action { (x, c) => c.copy(target = x) } text "Jar to inspect"
    }

    parser.parse(args, JarInfo()) match {
      case Some(info) => {
        val result: InspectionResult = try {
          if (!info.target.exists) {
            throw new Exception(s"Fail: Path `${info.target.toPath}` does not exist.")
          }

          inspectJarFile(info.target)
        } catch {
          case e: Exception => InspectionResult(success=false, error=Some(e.toString))
        }

        println(JsonProto.resultFormat.write(result).prettyPrint)
        sys.exit(if (result.success) 0 else 1)
      }
      case None => sys.exit(1)
    }
  }
}
