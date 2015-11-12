package com.memsql.spark.interface.util.python

import java.io.{PrintWriter, File}
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

/*
  Copied from org.apache.spark.api.python.PythonUtils
 */
object Utils {
  /** Get the PYTHONPATH for PySpark, either from SPARK_HOME, if it is set, or from our JAR */
  def sparkPythonPath: String = {
    val pythonPath = new ArrayBuffer[String]
    for (sparkHome <- sys.env.get("SPARK_HOME")) {
      pythonPath += Seq(sparkHome, "python", "lib", "pyspark.zip").mkString(File.separator)
      pythonPath += Seq(sparkHome, "python", "lib", "py4j-0.8.2.1-src.zip").mkString(File.separator)
      //NOTE: Modified for MemSQL Streamliner Python support
      pythonPath += Seq(sparkHome, "python", "lib", "pystreamliner.zip").mkString(File.separator)
    }
    pythonPath ++= SparkContext.jarOfObject(this)
    pythonPath.mkString(File.pathSeparator)
  }

  /** Merge PYTHONPATHS with the appropriate separator. Ignores blank strings. */
  def mergePythonPaths(paths: String*): String = {
    paths.filter(_ != "").mkString(File.pathSeparator)
  }

  def createPythonModule(moduleSuffix: String, code: String): File = {
    val pyFile = File.createTempFile("python", moduleSuffix)
    val writer = new PrintWriter(pyFile)
    writer.write(code)
    writer.close()

    pyFile
  }
}

