package com.memsql.spark.interface.util.python

import java.io.File
import java.nio.file.Files
import com.memsql.spark.etl.utils.Logging
import com.memsql.spark.interface.util.ProcessMonitor
import org.apache.commons.io.FilenameUtils

trait PythonPhaseProcess extends ProcessMonitor with Logging {
  def componentType: String
  def pyFile: File
  def className: String
  def javaPort: Int
  def pythonPort: Int

  val pythonModuleDir: String = pyFile.getParent
  val pythonClass: String = s"${FilenameUtils.removeExtension(pyFile.getName)}.$className"
  val pythonInterpreter: String = sys.env.get("PYSPARK_DRIVER_PYTHON").orElse(sys.env.get("PYSPARK_PYTHON")).getOrElse {
    throw new IllegalArgumentException("Must specify the PySpark driver")
  }
  val pythonExec = Seq(pythonInterpreter, "-m", "pystreamliner.entrypoint")

  // Build up a PYTHONPATH that includes the Spark assembly JAR (where this class is), the
  // python directories in SPARK_HOME (if set), and the file to run
  val pathElements = Seq(pythonModuleDir, Utils.sparkPythonPath, sys.env.getOrElse("PYTHONPATH", ""))
  val pythonPath = Utils.mergePythonPaths(pathElements: _*)

  override def cmd: Seq[String] = {
    pythonExec ++ Seq(componentType.toString, pythonClass, javaPort.toString, pythonPort.toString)
  }

  override def setup(builder: ProcessBuilder): Unit = {
    builder.redirectErrorStream(true) // combine stdout and stderr

    val env = builder.environment()
    env.put("PYTHONPATH", pythonPath)
    // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
    env.put("PYTHONUNBUFFERED", "YES") // value is needed to be set to a non-empty string
  }

  override def cleanup(): Unit = {
    try {
      Files.deleteIfExists(pyFile.toPath)
    } catch {
      case e: Exception => logWarn(s"Could not delete file $pyFile: ${e.getMessage}")
    }
  }
}

case class PythonExtractorProcess(pyFile: File, className: String, javaPort: Int, pythonPort: Int)
  extends PythonPhaseProcess {
  override val componentType: String = "extractor"
}

case class PythonTransformerProcess(pyFile: File, className: String, javaPort: Int, pythonPort: Int)
  extends PythonPhaseProcess {
  override val componentType: String = "transformer"
}
