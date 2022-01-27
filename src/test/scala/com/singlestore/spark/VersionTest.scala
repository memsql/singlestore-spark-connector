package com.singlestore.spark

import com.singlestore.spark.SQLGen.SinglestoreVersion
import org.scalatest.funspec.AnyFunSpec

class VersionTest extends AnyFunSpec {

  it("singlestore version test") {

    assert(SinglestoreVersion("7.0.1").atLeast("6.8.1"))
    assert(!SinglestoreVersion("6.8.1").atLeast("7.0.1"))
    assert(SinglestoreVersion("7.0.2").atLeast("7.0.1"))
    assert(SinglestoreVersion("7.0.10").atLeast("7.0.9"))
    assert(SinglestoreVersion("7.2.5").atLeast("7.1.99999"))
    assert(SinglestoreVersion("7.2.500").atLeast("7.2.499"))
  }
}
