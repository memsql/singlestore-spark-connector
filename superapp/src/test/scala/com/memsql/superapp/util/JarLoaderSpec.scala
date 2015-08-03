package com.memsql.superapp.util

import com.memsql.superapp.UnitSpec

class JarLoaderSpec extends UnitSpec {
  "JarLoader" should "load class from a jar" in {
    val clazz = JarLoader.loadClass("http://coreos-10.memcompute.com:8080/repository/internal/com/memsql/memsql/0.1.2/memsql-0.1.2.jar",
      "com.memsql.spark.etl.api.Loader")
    assert(Paths.exists("test_root/jars/memsql-0.1.2.jar"))
    assert(clazz.getPackage.getName == "com.memsql.spark.etl.api")
  }

  it should "error if jar doesn't exist" in {
    intercept[FileDownloadException] {
      JarLoader.loadClass("http://notawebsite/jars/foo1.jar", "com.memsql.foo.bar")
    }
    assert(!Paths.exists(Paths.join(Paths.JAR_DIR, "foo1.jar")))

    intercept[FileDownloadException] {
      JarLoader.loadClass("http://coreos-10.memcompute.com:8080/not_a_directory/foo2.jar", "com.memsql.foo.bar")
    }
    assert(!Paths.exists(Paths.join(Paths.JAR_DIR, "foo2.jar")))

    intercept[FileDownloadException] {
      JarLoader.loadClass("file:///do_not_put_jars_in_this_directory_or_test_will_fail/foo3.jar", "com.memsql.foo.bar")
    }
    assert(!Paths.exists(Paths.join(Paths.JAR_DIR, "foo3.jar")))
  }

  it should "error if class doesn't exist" in {
    intercept[ClassLoadException] {
      JarLoader.loadClass("http://coreos-10.memcompute.com:8080/repository/internal/com/memsql/memsql/0.1.2/memsql-0.1.2.jar",
        "com.memsql.class_does_not_exist.foo")
    }
  }
}
