package com.singlestore.spark

import java.sql.DriverManager

import com.singlestore.spark.SQLGen.SinglestoreVersion
import org.scalatest.funspec.AnyFunSpec

class VersionTest extends AnyFunSpec {

  it("singlestore version test") {
    val conn = DriverManager.getConnection("jdbc:singlestore://localhost:5506?user=root&password=1")
    val stmt = conn.prepareStatement("DROP TABLE IF EXISTS testdb.LineStringCodec")
    val rs   = stmt.executeQuery()
    rs.close()
  }
}
