package com.singlestore.spark

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class SinglestoreOptionsTest extends IntegrationSuiteBase {
  val requiredOptions = Map("ddlEndpoint" -> "h:3306")

  describe("equality") {
    it("should sort dmlEndpoints") {
      assert(
        SinglestoreOptions(
          CaseInsensitiveMap(
            requiredOptions ++ Map("dmlEndpoints" -> "host1:3302,host2:3302,host1:3342"))) ==
          SinglestoreOptions(
            CaseInsensitiveMap(
              requiredOptions ++ Map("dmlEndpoints" -> "host2:3302,host1:3302,host1:3342"))),
        "Should sort dmlEndpoints"
      )
    }
  }

  describe("splitEscapedColumns") {
    it("empty string") {
      assert(SinglestoreOptions.splitEscapedColumns("") == List())
    }

    it("3 columns") {
      assert(
        SinglestoreOptions.splitEscapedColumns("col1,col2,col3") == List("col1", "col2", "col3"))
    }

    it("with spaces") {
      assert(
        SinglestoreOptions
          .splitEscapedColumns("  col1 , col2,   col3") == List("  col1 ", " col2", "   col3"))
    }

    it("with backticks") {
      assert(
        SinglestoreOptions.splitEscapedColumns(" ` col1` , `col2`, ``  col3") == List(" ` col1` ",
                                                                                      " `col2`",
                                                                                      " ``  col3"))
    }

    it("with commas inside of backticks") {
      assert(
        SinglestoreOptions
          .splitEscapedColumns(" ` ,, col1,` , ``,```,col3`, ``  col4,`,,`") == List(
          " ` ,, col1,` ",
          " ``",
          "```,col3`",
          " ``  col4",
          "`,,`"))
    }
  }

  describe("trimAndUnescapeColumn") {
    it("empty string") {
      assert(SinglestoreOptions.trimAndUnescapeColumn("") == "")
    }

    it("spaces") {
      assert(SinglestoreOptions.trimAndUnescapeColumn("   ") == "")
    }

    it("in backticks") {
      assert(SinglestoreOptions.trimAndUnescapeColumn(" `asd`  ") == "asd")
    }

    it("backticks in the result") {
      assert(SinglestoreOptions.trimAndUnescapeColumn(" ```a``sd`  ") == "`a`sd")
    }

    it("several escaped words") {
      assert(SinglestoreOptions.trimAndUnescapeColumn(" ```a``sd` ```a``sd` ") == "`a`sd `a`sd")
    }

    it("backtick in the middle of string") {
      assert(
        SinglestoreOptions
          .trimAndUnescapeColumn(" a```a``sd` ```a``sd` ") == "a```a``sd` ```a``sd`")
    }
  }
}
