package com.memsql.spark.interface.util.python

import com.memsql.spark.interface.UnitSpec
import org.apache.commons.io.{FilenameUtils, IOUtils}

class UtilsSpec extends UnitSpec {
  "Utils" should "write Python code to temporary module" in {
    val pyFile = Utils.createPythonModule("a_module.py",
      """
        |def test(x):
        | print(x + 12)
      """.stripMargin)
    assert(pyFile.exists)
    pyFile.deleteOnExit()
    val pyModule = FilenameUtils.removeExtension(pyFile.getName)

    val entryPoint = Utils.createPythonModule("entrypoint.py",
      s"""
        |import importlib
        |
        |module = importlib.import_module("$pyModule")
        |module.test(1)
      """.stripMargin)
    assert(entryPoint.exists)

    val process = Runtime.getRuntime.exec(s"python ${entryPoint.getPath}")
    assert(process.waitFor() == 0)

    val stdout = IOUtils.toString(process.getInputStream)
    assert(stdout == "13\n")

    val stderr = IOUtils.toString(process.getErrorStream)
    assert(stderr == "")
  }
}
