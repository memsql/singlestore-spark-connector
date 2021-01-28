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
}
