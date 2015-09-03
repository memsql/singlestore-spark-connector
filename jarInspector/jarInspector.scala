package com.memsql.jar_inspector

import org.clapper.classutil.{ClassInfo, ClassFinder}
import java.io.File
import spray.json._

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
  var VERSION = "0.0.1"

  val EXTRACTOR_CLASSES = List(
    "com.memsql.spark.etl.api.Extractor",
    "com.memsql.spark.etl.api.ByteArrayExtractor"
  )

  val TRANSFORMER_CLASSES = List(
    "com.memsql.spark.etl.api.Transformer",
    "com.memsql.spark.etl.api.ByteArrayTransformer"
  )

  val ALL_CLASSES = EXTRACTOR_CLASSES ++ TRANSFORMER_CLASSES

  private def isSubclass(ancestors: List[String], classInfo: ClassInfo): Boolean = {
    ancestors.contains(classInfo.superClassName)
  }

  def inspectJarFile(jarFile: File): InspectionResult = {
    val classes = ClassFinder(List(jarFile)).getClasses.filter(isSubclass(ALL_CLASSES, _))

    InspectionResult(
      success = true,
      extractors = classes.filter(isSubclass(EXTRACTOR_CLASSES, _)).map(_.name).toList,
      transformers = classes.filter(isSubclass(TRANSFORMER_CLASSES, _)).map(_.name).toList
    )
  }

  def main(args: Array[String]): Unit = {
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
