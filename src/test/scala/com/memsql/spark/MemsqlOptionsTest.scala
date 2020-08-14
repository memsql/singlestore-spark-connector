package com.memsql.spark

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class MemsqlOptionsTest extends IntegrationSuiteBase {
  val requiredOptions = Map("ddlEndpoint" -> "h:3306")

  describe("equality") {
    it("should sort dmlEndpoints") {
      assert(
        MemsqlOptions(
          CaseInsensitiveMap(
            requiredOptions ++ Map("dmlEndpoints" -> "host1:3302,host2:3302,host1:3342"))) ==
          MemsqlOptions(
            CaseInsensitiveMap(
              requiredOptions ++ Map("dmlEndpoints" -> "host2:3302,host1:3302,host1:3342"))),
        "Should sort dmlEndpoints"
      )
    }
  }
}
