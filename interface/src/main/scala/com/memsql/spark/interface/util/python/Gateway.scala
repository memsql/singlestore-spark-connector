package com.memsql.spark.interface.util.python

import java.net.{BindException, ServerSocket}

import com.memsql.spark.interface.util.BaseException
import com.memsql.spark.phases.api.python.{PythonTransformerInterface, PythonExtractorInterface}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import py4j.{GatewayServer, Py4JException, Py4JJavaException, Py4JNetworkException}
import org.apache.log4j.{Logger, Level}

class PythonGatewayException(message: String, nestedException: Exception) extends BaseException(message, nestedException) {
  def this(message: String) = this(message, null)
}

class PythonTraceback(message: String) extends PythonGatewayException(message)

class PythonGatewayEntryPoint(sc: SparkContext) {
  val jsc: JavaSparkContext = new JavaSparkContext(sc)

  var extractor: PythonExtractorInterface = null
  var transformer: PythonTransformerInterface = null

  def registerExtractor(pyExtractor: PythonExtractorInterface): Unit = {
    extractor = pyExtractor
  }

  def registerTransformer(pyTransformer: PythonTransformerInterface): Unit = {
    transformer = pyTransformer
  }

  /**
   * Used to track the last Python exception that was thrown.
   */
  var lastTraceback: String = null

  def setTraceback(traceback: String): Unit = {
    lastTraceback = traceback
  }

  def resetTraceback(): Unit = {
    lastTraceback = null
  }
}

object PythonGateway {
  val MIN_PORT_VALUE: Int = 1024 // scalastyle:ignore
  val MAX_PORT_VALUE: Int = 65535 // scalastyle:ignore
  val PORT_RANGE: Seq[Int] = MIN_PORT_VALUE to MAX_PORT_VALUE

  def findFreePorts(num: Int, portSelection: Seq[Int]): Seq[Int] = {
    val availablePorts = portSelection.map(port => {
      try {
        val socket = new ServerSocket(port)
        socket.close()
        Some(port)
      } catch {
        case e: BindException => None
      }
    })

    val freePorts = availablePorts.flatten.take(num)
    if (freePorts.length < num) {
      throw new PythonGatewayException(s"Could not find $num free ports")
    }

    freePorts
  }
}

class PythonGateway(sc: SparkContext) {
  val entryPoint: PythonGatewayEntryPoint = new PythonGatewayEntryPoint(sc)
  var gateway: GatewayServer = null

  var port: Int = -1
  var pythonPort: Int = -1

  def start: Unit = {
    // ensure we don't have port collisions by synchronizing
    PythonGateway.synchronized {
      val ports = PythonGateway.findFreePorts(2, PythonGateway.PORT_RANGE)
      port = ports(0)
      pythonPort = ports(1)

      gateway = new GatewayServer(entryPoint, port, pythonPort, 0, 0, null)
      gateway.start
    }
  }

  def stop: Unit = gateway.shutdown

  def enableDebugLogging(): Unit = {
    GatewayServer.turnLoggingOn()
    val logger = Logger.getLogger("py4j")
    logger.setLevel(Level.ALL)
  }

  /**
   * Calls the function and wraps any [[py4j.Py4JException]]s with the corresponding Python stack trace, if it exists.
   *
   * Note that [[py4j.Py4JJavaException]] and [[py4j.Py4JNetworkException]]s are passed through
   * because there is no additional context to provide.
   *
   * @param f The lambda to run
   */
  def wrapPythonExceptions[T](f: => T): T = {
    try {
      f
    } catch {
      case e: Py4JNetworkException => throw e
      case e: Py4JJavaException => throw e
      case e: Py4JException => {
        // if there is a stack trace from the Python side include it, otherwise rethrow the exception
        entryPoint.lastTraceback match {
          case null => throw e
          case _ => throw new PythonTraceback(s"""
            |Encountered an exception in Python:
            |${entryPoint.lastTraceback}
            |
            |Java Exception:
            """.stripMargin)
        }
      }
    } finally {
      entryPoint.resetTraceback()
    }
  }
}
