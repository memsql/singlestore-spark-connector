package com.memsql.spark.interface.util.python

import scala.concurrent.duration._
import com.memsql.spark.etl.utils.Logging
import com.memsql.spark.interface.util.BaseException
import org.apache.spark.SparkContext

class PythonPhaseTimeoutException(message: String) extends BaseException(message)

/*
  Wrapper for Py4J-backed Phases
 */
trait PythonPhase[T] extends Logging {
  def sc: SparkContext
  def pipelineId: String
  def getPhaseObject(): T
  def name: String

  val gateway = new PythonGateway(sc)
  gateway.start
  logInfo(s"Started Python gateway for pipeline $pipelineId on ports ${gateway.port} and ${gateway.pythonPort}")

  def init(pythonProcess: PythonPhaseProcess): T = {
    pythonProcess.start
    logInfo(s"Started Python $name process for pipeline $pipelineId")

    val startTime: Long = System.currentTimeMillis
    val maxInterval: Duration = 30.seconds
    logInfo(s"Waiting for Python $name to be registered")
    while (getPhaseObject() == null) {
      pythonProcess.ensureAlive
      Thread.sleep(1000) // scalastyle:ignore

      if (System.currentTimeMillis > startTime + maxInterval.toMillis) {
        throw new PythonPhaseTimeoutException(s"Python process $name took longer than ${maxInterval.toSeconds} seconds to be registered")
      }
    }

    getPhaseObject()
  }
}
