package com.memsql.spark

import org.scalatest.funspec.AnyFunSpec

class SQLGenTest extends AnyFunSpec {
  import SQLGen._

  it("works") {
    /*
    val query =
      select(alias(IntVar(1), "foo"))
        .from(Ident("bar"))

    assert(query.sql == "SELECT ( ? ) AS `foo` FROM `bar`")
     */
    // TODO: verify that sql codegen works
  }

}
