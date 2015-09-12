package com.memsql.spark.interface.util

import org.apache.log4j._
import com.memsql.spark.interface.UnitSpec

class PipelineLoggerSpec extends UnitSpec {
  "PipelineLogger" should "put log entries in a list" in {
    val logger1 = new PipelineLogger("Test Logger")
    logger1.setLevel(Level.INFO)

    logger1.info("Test info message")
    logger1.warn("Test warn message")
    logger1.debug("Test debug message")

    val logEntries1 = logger1.getLogEntries
    // Note that we should not include the debug log because logger1's level
    // is set too high.
    assert(logEntries1.length == 2)
    assert(logEntries1(0).contains("INFO: Test info message"))
    assert(logEntries1(1).contains("WARN: Test warn message"))

    val logger2 = new PipelineLogger("test logger")
    logger2.setLevel(Level.DEBUG)

    logger2.info("Test info message")
    logger2.warn("Test warn message")
    logger2.debug("Test debug message")

    val logEntries2 = logger2.getLogEntries
    assert(logEntries2.length == 3)
    assert(logEntries2(0).contains("INFO: Test info message"))
    assert(logEntries2(1).contains("WARN: Test warn message"))
    assert(logEntries2(2).contains("DEBUG: Test debug message"))
  }
}
