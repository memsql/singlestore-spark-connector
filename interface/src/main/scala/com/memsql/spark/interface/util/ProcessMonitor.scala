package com.memsql.spark.interface.util

import java.io.InputStream

import org.apache.commons.io.IOUtils

class ProcessMonitorException(message: String) extends BaseException(message)

trait ProcessMonitor {
  def cmd: Seq[String]
  def setup(builder: ProcessBuilder): Unit = {}
  def cleanup(): Unit = {}

  var process: Process = null

  def start: Unit = {
    if (process != null) {
      throw new ProcessMonitorException(s"Process ${cmd.mkString(" ")} has already been started")
    }

    val builder: ProcessBuilder = new ProcessBuilder(cmd: _*)
    setup(builder)

    process = builder.start()
  }

  def isAlive: Boolean = {
    process match {
      case null => throw new ProcessMonitorException(s"Cannot check status of process that has not been started")
      case _ => Runtime.getRuntime.exec(s"ps -o pid= -p $pid").waitFor() == 0
    }
  }

  def ensureAlive: Unit = {
    if (!isAlive) {
      val processOutput = getOutput

      // clean up any files after retrieving process output
      cleanup()

      throw new ProcessMonitorException(s"Process terminated with exit code ${process.exitValue()}: $processOutput")
    }
  }

  def getOutput: String = IOUtils.toString(process.getInputStream)

  def stop: Int = {
    Runtime.getRuntime.exec(s"kill -INT $pid")
    val exitCode = process.waitFor()
    cleanup()
    exitCode
  }

  // java.lang.UNIXProcess has a private pid field - use reflection to retrieve the pid for this process.
  def pid: Int = {
    process match {
      case null => throw new ProcessMonitorException("Cannot get PID of process that has not been started")
      case default => {
        try {
          val clazz = process.getClass
          val f = clazz.getDeclaredField("pid")
          if (!f.isAccessible) {
            f.setAccessible(true)
          }

          f.getInt(process)
        } catch {
          case e: Exception =>
            throw new ProcessMonitorException(s"Could not retrieve PID of process ${cmd.mkString(" ")}: ${e.getMessage}")
        }
      }
    }
  }
}

