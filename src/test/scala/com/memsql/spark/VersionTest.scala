package com.memsql.spark

import com.memsql.spark.SQLGen.MemsqlVersion
import org.scalatest.funspec.AnyFunSpec

class VersionTest extends AnyFunSpec {

  it("memsql version test") {

    assert(MemsqlVersion("7.0.1").atLeast("6.8.1"))
    assert(!MemsqlVersion("6.8.1").atLeast("7.0.1"))
    assert(MemsqlVersion("7.0.2").atLeast("7.0.1"))
    assert(MemsqlVersion("7.0.10").atLeast("7.0.9"))
    assert(MemsqlVersion("7.2.5").atLeast("7.1.99999"))
    assert(MemsqlVersion("7.2.500").atLeast("7.2.499"))
  }
}
