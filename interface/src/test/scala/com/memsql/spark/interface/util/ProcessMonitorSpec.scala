package com.memsql.spark.interface.util

import com.memsql.spark.interface.UnitSpec
import com.memsql.spark.interface.util.python.Utils
import org.apache.commons.io.IOUtils

class ProcessMonitorSpec extends UnitSpec {
  "ProcessMonitor" should "track the process status" in {
    val monitor = new ProcessMonitor {
      override def cmd: Seq[String] = Seq(
        "/bin/bash", "-c", "sleep 0.2"
      )
    }

    intercept[ProcessMonitorException] {
      monitor.isAlive
    }
    monitor.start
    assert(monitor.isAlive)
    monitor.stop
    assert(!monitor.isAlive)
  }

  it should "return the correct exit code" in {
    var monitor = new ProcessMonitor {
      override def cmd: Seq[String] = Seq("/bin/bash", "-c", "echo uh; exit 12")
    }

    monitor.start
    Thread.sleep(100) //scalastyle:ignore
    assert(monitor.stop == 12)
    assert(!monitor.isAlive)

    monitor = new ProcessMonitor {
      override def cmd: Seq[String] = Seq("/bin/bash", "-c", "echo hey; exit 0")
    }

    monitor.start
    Thread.sleep(100) //scalastyle:ignore
    assert(monitor.stop == 0)
    assert(!monitor.isAlive)
  }

  it should "send SIGINT to a process on shutdown" in {
    val pyFile = Utils.createPythonModule("test.py",
      """
        |import sys
        |import time
        |import signal
        |
        |def handler(*args, **kwargs):
        |  print("GOT SIGINT")
        |  sys.exit(17)
        |
        |signal.signal(signal.SIGINT, handler)
        |
        |while True:
        |  print("running")
        |  time.sleep(0.1)
      """.stripMargin)
    assert(pyFile.exists)

    val monitor = new ProcessMonitor {
      override def cmd: Seq[String] = Seq("python", pyFile.getPath)
    }

    monitor.start
    assert(monitor.isAlive)
    // wait before stopping the process
    Thread.sleep(500) //scalastyle:ignore

    assert(monitor.stop == 17)
    assert(!monitor.isAlive)

    val stdout = IOUtils.toString(monitor.process.getInputStream)
    assert(stdout.contains("running"))
    assert(stdout.contains("GOT SIGINT"))

    val stderr = IOUtils.toString(monitor.process.getErrorStream)
    assert(stderr == "")
  }
}
