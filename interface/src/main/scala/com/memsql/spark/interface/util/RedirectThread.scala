package com.memsql.spark.interface.util

import java.io.{IOException, OutputStream, InputStream}

/**
 * A utility class to redirect the child process's stdout or stderr.
 *
 * Copied from org.apache.spark.util.RedirectThread with minor modifications
 */
class RedirectThread(in: InputStream, out: OutputStream, name: String)
  extends Thread(name) {
  setDaemon(true)

  override def run() {
    scala.util.control.Exception.ignoring(classOf[IOException]) {
      // FIXME: We copy the stream on the level of bytes to avoid encoding problems.
      val buf = new Array[Byte](1024) // scalastyle:ignore
      var len = in.read(buf)
      while (len != -1) {
        out.write(buf, 0, len)
        out.flush()
        len = in.read(buf)
      }
    }
  }
}
