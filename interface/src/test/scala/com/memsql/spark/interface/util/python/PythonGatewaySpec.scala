package com.memsql.spark.interface.util.python

import java.net.ServerSocket

import com.memsql.spark.interface.UnitSpec

class PythonGatewaySpec extends UnitSpec {
  "PythonGateway" should "bind to available ports" in {
    val freePorts = PythonGateway.findFreePorts(200, PythonGateway.PORT_RANGE) //scalastyle:ignore

    for (port <- freePorts) {
      val server = new ServerSocket(port)
      server.close()
    }
  }

  it should "throw if there are no available ports" in {
    // first find an available port
    val freePorts = PythonGateway.findFreePorts(1, PythonGateway.PORT_RANGE)
    assert(freePorts.size == 1)

    // then claim it and make sure it isn't marked as free again
    val server = new ServerSocket(freePorts.head)

    try {
      val exc = intercept[PythonGatewayException] {
        PythonGateway.findFreePorts(1, Seq(freePorts.head))
      }

      assert(exc.getMessage.contains("Could not find 1 free port"))
    } finally {
      server.close()
    }
  }
}
