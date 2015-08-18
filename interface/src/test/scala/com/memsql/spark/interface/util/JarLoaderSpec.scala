package com.memsql.spark.interface.util

import java.io.File

import com.memsql.spark.interface._

class JarLoaderSpec extends UnitSpec {
  "JarLoader" should "load class from a jar" in {
    val localJarFile = s"target/scala-2.10/MemSQL-assembly-${Main.VERSION}.jar"

    var clazz = JarLoader.loadClass("http://coreos-10.memcompute.com:8080/repository/internal/com/memsql/memsql/0.1.2/memsql-0.1.2.jar",
      "com.memsql.spark.etl.api.Loader")
    assert(Paths.exists("test_root/jars/memsql-0.1.2.jar"))
    assert(clazz.getPackage.getName == "com.memsql.spark.etl.api")

    clazz = JarLoader.loadClass(Paths.join(new File(".").getCanonicalPath, localJarFile),
      "com.memsql.spark.etl.api.Loader")
    assert(clazz.getPackage.getName == "com.memsql.spark.etl.api")
    assert(clazz.getCanonicalName == "com.memsql.spark.etl.api.Loader")
  }

  it should "error if jar doesn't exist" in {
    intercept[FileDownloadException] {
      JarLoader.getClassLoader("http://notawebsite/jars/foo1.jar")
    }
    assert(!Paths.exists(Paths.join(Paths.JAR_DIR, "foo1.jar")))

    intercept[FileDownloadException] {
      JarLoader.getClassLoader("http://coreos-10.memcompute.com:8080/not_a_directory/foo2.jar")
    }
    assert(!Paths.exists(Paths.join(Paths.JAR_DIR, "foo2.jar")))

    intercept[FileDownloadException] {
      JarLoader.getClassLoader("file:///do_not_put_jars_in_this_directory_or_test_will_fail/foo3.jar")
    }
    assert(!Paths.exists(Paths.join(Paths.JAR_DIR, "foo3.jar")))
  }

  it should "error if class doesn't exist" in {
    val classLoader = JarLoader.getClassLoader("http://coreos-10.memcompute.com:8080/repository/internal/com/memsql/memsql/0.1.2/memsql-0.1.2.jar")

    intercept[ClassLoadException] {
      JarLoader.loadClass(classLoader, "com.memsql.class_does_not_exist.foo")
    }
  }
}
