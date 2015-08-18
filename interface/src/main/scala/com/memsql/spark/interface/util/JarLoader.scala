package com.memsql.spark.interface.util

import com.memsql.spark.etl.utils.Logging

import sys.process._
import java.io.File
import java.net.{URL, URLClassLoader}

class JarLoaderException(message:String, nestedException:Exception) extends BaseException(message:String, nestedException:Exception)
case class FileDownloadException(message:String, nestedException:Exception)
  extends JarLoaderException(message:String, nestedException:Exception)
case class ClassLoadException(message:String, nestedException:Exception)
  extends JarLoaderException(message:String, nestedException:Exception)

object JarLoader extends Logging {
  def getClassLoader(path: String): ClassLoader = {
    var url: URL = null

    var file = new File(path)
    if (file.isFile) {
      logInfo(s"Loading jar from local file at $path")
      url = file.toURI.toURL
    } else {
      try {
        //TODO: intelligently cache jars instead of downloading every time
        val target = Paths.join(Paths.JAR_DIR, file.getName)
        url = new URL(s"file://$target")

        logInfo(s"Downloading jar from $path to $target")
        file = new File(target)
        new URL(path) #> file !!
      } catch {
        case downloadException: Exception => {
          try {
            file.delete
          } catch {
            case e: Exception => //ignore any errors deleting the failed downloads
          }

          throw new FileDownloadException(s"Could not load `$path`: $downloadException", downloadException)
        }
      }
    }

    try {
      new URLClassLoader(Array(url), this.getClass.getClassLoader)
    } catch {
      case e: Exception => throw new JarLoaderException("Unexpected error", e)
    }
  }

  def loadClass(path:String, clazz:String): Class[_] = {
    val classLoader = getClassLoader(path)
    loadClass(classLoader, clazz)
  }

  def loadClass(classLoader: ClassLoader, clazz:String): Class[_] = {
    try {
      classLoader.loadClass(clazz)
    } catch {
      case e: ClassNotFoundException => throw new ClassLoadException(s"Class `$clazz` not found", e)
      case e: Exception => throw new JarLoaderException("Unexpected error", e)
    }
  }
}
