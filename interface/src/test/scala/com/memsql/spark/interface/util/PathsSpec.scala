package com.memsql.spark.interface.util

import com.memsql.spark.interface._

class PathsSpec extends UnitSpec {
  "Paths" should "initialize to use the base directory" in {
    Paths.initialize("test_root/foo")
    assert(Paths.BASE_DIR == "test_root/foo")
    assert(Paths.exists("test_root/foo"))
    assert(Paths.JAR_DIR == "test_root/foo/jars")
    assert(Paths.exists("test_root/foo/jars"))

    Paths.initialize("test_root/bar")
    assert(Paths.BASE_DIR == "test_root/bar")
    assert(Paths.exists("test_root/bar"))
    assert(Paths.JAR_DIR == "test_root/bar/jars")
    assert(Paths.exists("test_root/bar/jars"))
  }

  it should "join directories" in {
    assert(Paths.join("", "") === "/")
    assert(Paths.join("", "bar") === "/bar")
    assert(Paths.join("foo", "") === "foo")
    assert(Paths.join("foo", "bar") === "foo/bar")
  }

  it should "test for path existence" in {
    assert(Paths.exists("test_root"))
    assert(!Paths.exists("/not_a_directory/test_root"))
  }

  it should "error when directories cannot be created" in {
    intercept[PathException] {
      Paths.initialize("/")
    }
  }
}
