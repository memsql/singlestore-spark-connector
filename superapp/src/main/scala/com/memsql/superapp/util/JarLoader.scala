package com.memsql.superapp.util

import sys.process._
import java.io.File
import java.net.{URL, URLClassLoader}

class JarLoaderException(message:String, nestedException:Exception) extends BaseException(message:String, nestedException:Exception)
case class FileDownloadException(message:String, nestedException:Exception)
  extends JarLoaderException(message:String, nestedException:Exception)
case class ClassLoadException(message:String, nestedException:Exception)
  extends JarLoaderException(message:String, nestedException:Exception)

object JarLoader {
  def loadClass(path:String, clazz:String): Class[_] = {
    var url: URL = null

    var file = new File(path)
    if (file.isFile) {
      url = file.toURI.toURL
    } else {
      try {
        //TODO: intelligently cache jars instead of downloading every time
        val target = Paths.join(Paths.JAR_DIR, file.getName)
        url = new URL(s"file://$target")

        Console.println(s"Downloading jar from $path to $target")
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
      val classLoader = new URLClassLoader(Array(url), this.getClass.getClassLoader)
      classLoader.loadClass(clazz)
    } catch {
      case e: ClassNotFoundException => throw new ClassLoadException(s"Class `$clazz` not found", e)
      case e: Exception => throw new JarLoaderException("Unexpected error", e)
    }
  }
}
