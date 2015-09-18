package com.memsql.jar_inspector

import java.io.File
import java.util.Properties
import spray.json._
import org.apache.log4j.PropertyConfigurator

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.reflections.util.ConfigurationBuilder
import org.reflections.Reflections
import org.reflections.scanners._
import org.reflections.vfs._
import com.memsql.spark.etl.Meta

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

class JarInspectorException(message: String) extends Exception(message)

class MemSQLVersionScanner extends AbstractScanner {
  override def scan(file: Vfs.File, classObject: Object): Object = {
    if (file.getName.contains(Meta.versionFileName)) {
      val version = Meta.readVersionFromResource(file.openInputStream)
      getStore.put("version", version)
    }
    classObject
  }

  override def scan(cls: Object) {
    throw new UnsupportedOperationException() //shouldn't get here
  }
}

object JarInspector {
  val MAX_SUBCLASSES = 1024
  val VERSION = "0.0.2"

  val EXTRACTOR_ROOT_CLASS = "com.memsql.spark.etl.api.Extractor"
  val TRANSFORMER_ROOT_CLASS = "com.memsql.spark.etl.api.Transformer"

  // We won't include any extractors or transformers that begin with this
  // prefix in the output
  val IGNORE_PREFIX = "com.memsql.spark.etl"

  def getAllSubclasses(subTypesMap: Map[String, List[String]], rootClass: String): List[String] = {
    var index = 0
    val result = mutable.ListBuffer[String](rootClass)
    while (index < result.length) {
      subTypesMap.get(result(index)).foreach(result.appendAll(_))
      index += 1
      if (index > MAX_SUBCLASSES) {
        throw new JarInspectorException(s"JAR file contains more than $MAX_SUBCLASSES implementations of $rootClass")
      }
    }
    result.filterNot(_.startsWith(IGNORE_PREFIX)).toList
  }

  def inspectJarFile(jarFile: File): InspectionResult = {
    val config = new ConfigurationBuilder()
      .setUrls(List(jarFile.toURI.toURL))
      .setScanners(new SubTypesScanner(false), new MemSQLVersionScanner())

    val reflect = new Reflections(config)

    val inspectorVersion = Meta.version
    val scannedVersions = reflect.getStore.get("MemSQLVersionScanner").asMap
    if (!scannedVersions.contains("version")) {
      throw new JarInspectorException(s"JAR file does not include MemSQL etlib.")
    }

    val jarVersion = scannedVersions.get("version").head
    val jarVersionTuple = jarVersion.split("\\.").take(2).toList
    val inspectorVersionTuple = inspectorVersion.split("\\.").take(2).toList
    //NOTE: we only compare major/minor versions here because they may have changed the etl interface
    if (jarVersionTuple != inspectorVersionTuple) {
      throw new JarInspectorException(s"JAR file was compiled against MemSQL etlib version $jarVersion but this MemSQL Spark distribution is version $inspectorVersion")
    }

    val subTypesMap = mapAsScalaMap(reflect.getStore.get("SubTypesScanner").asMap)
      .map({ x => (x._1, x._2.toList)}).toMap

    val extractors = getAllSubclasses(subTypesMap, EXTRACTOR_ROOT_CLASS)
    val transformers = getAllSubclasses(subTypesMap, TRANSFORMER_ROOT_CLASS)

    if (extractors.length == 0 && transformers.length == 0) {
      throw new JarInspectorException(s"JAR file does not contain any valid Extractor or Transformer implementations.")
    }

    InspectionResult(success = true, extractors = extractors, transformers = transformers)
  }

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("log4j.rootCategory", "INFO, console")
    props.setProperty("log4j.appender.console", "org.apache.log4j.varia.NullAppender")
    PropertyConfigurator.configure(props)

    val parser = new scopt.OptionParser[JarInfo]("jar-inspector") {
      override def showUsageOnError = true

      head("jarInspector", JarInspector.VERSION)
      opt[File]("target") required() action { (x, c) => c.copy(target = x) } text "JAR to inspect"
    }

    parser.parse(args, JarInfo()) match {
      case Some(info) => {
        val result: InspectionResult = try {
          if (!info.target.exists) {
            throw new JarInspectorException(s"Fail: Path `${info.target.toPath}` does not exist.")
          }

          inspectJarFile(info.target)
        } catch {
          case e: Exception => InspectionResult(success=false, error=Some(e.getMessage))
        }

        println(JsonProto.resultFormat.write(result).prettyPrint)
        sys.exit(if (result.success) 0 else 1)
      }
      case None => sys.exit(1)
    }
  }
}
