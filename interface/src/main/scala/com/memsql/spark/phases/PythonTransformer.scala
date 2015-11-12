package com.memsql.spark.phases

import java.io.File
import com.memsql.spark.etl.api.{PhaseConfig, Transformer}
import com.memsql.spark.etl.utils.PhaseLogger
import com.memsql.spark.interface.util.python.{Utils, PythonPhase, PythonTransformerProcess, PythonPhaseProcess}
import com.memsql.spark.phases.api.python.PythonTransformerInterface
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

case class PythonTransformConfig(code: String, class_name: String) extends PhaseConfig

/*
  Wrapper for Py4J-backed Transformers
 */
class PythonTransformer(override val sc: SparkContext, override val pipelineId: String, config: PythonTransformConfig) extends Transformer
with PythonPhase[PythonTransformerInterface] {
  override def name: String = config.class_name

  override def getPhaseObject(): PythonTransformerInterface = {
    gateway.entryPoint.transformer
  }

  val pyFile: File = Utils.createPythonModule("transformer.py", config.code)
  val pythonProcess: PythonPhaseProcess = PythonTransformerProcess(pyFile, config.class_name, gateway.port, gateway.pythonPort)
  val pyTransformer: PythonTransformerInterface = init(pythonProcess)

  override def initialize(sqlContext: SQLContext, config: PhaseConfig, logger: PhaseLogger): Unit = gateway.wrapPythonExceptions {
    pyTransformer.Py4JInitialize(sqlContext, "{}", logger)
  }

  override def cleanup(sqlContext: SQLContext, config: PhaseConfig, logger: PhaseLogger): Unit = gateway.wrapPythonExceptions {
    if (pythonProcess.isAlive) {
      pyTransformer.Py4JCleanup(sqlContext, "{}", logger)
      pythonProcess.stop
    }
  }

  override def transform(sqlContext: SQLContext, df: DataFrame, config: PhaseConfig, logger: PhaseLogger): DataFrame =
    gateway.wrapPythonExceptions {
    pyTransformer.Py4JTransform(sqlContext, df, "{}", logger)
  }
}
