package com.memsql.spark.phases

import java.io.File
import com.memsql.spark.etl.api.{PhaseConfig, Extractor}
import com.memsql.spark.etl.utils.PhaseLogger
import com.memsql.spark.interface.util.python.{Utils, PythonExtractorProcess, PythonPhaseProcess, PythonPhase}
import com.memsql.spark.phases.api.python.PythonExtractorInterface
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext

case class PythonExtractConfig(code: String, class_name: String) extends PhaseConfig

/*
  Wrapper for Py4J-backed Extractors
 */
class PythonExtractor(override val sc: SparkContext, override val pipelineId: String, config: PythonExtractConfig) extends Extractor
with PythonPhase[PythonExtractorInterface] {
  override def name: String = config.class_name

  override def getPhaseObject(): PythonExtractorInterface = {
    gateway.entryPoint.extractor
  }

  val pyFile: File = Utils.createPythonModule("extractor.py", config.code)
  val pythonProcess: PythonPhaseProcess = PythonExtractorProcess(pyFile, config.class_name, gateway.port, gateway.pythonPort)
  val pyExtractor: PythonExtractorInterface = init(pythonProcess)

  override def initialize(ssc: StreamingContext, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
                          logger: PhaseLogger): Unit = gateway.wrapPythonExceptions {
    pyExtractor.Py4JInitialize(ssc, sqlContext, "{}", batchInterval, logger)
  }

  override def cleanup(ssc: StreamingContext, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
                       logger: PhaseLogger): Unit = gateway.wrapPythonExceptions {
    if (pythonProcess.isAlive) {
      pyExtractor.Py4JCleanup(ssc, sqlContext, "{}", batchInterval, logger)
      pythonProcess.stop
    }
  }

  override def next(ssc: StreamingContext, time: Long, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
                    logger: PhaseLogger): Option[DataFrame] = gateway.wrapPythonExceptions {
    Option(pyExtractor.Py4JNext(ssc, time, sqlContext, "{}", batchInterval, logger))
  }
}
