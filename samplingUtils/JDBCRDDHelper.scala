package org.apache.spark.sql.execution.datasources.jdbc

import java.util.Properties
import org.apache.spark.sql.types.StructType

object JDBCRDDHelper {
  // This object allows us to access private members of JDBCRDD (e.g the
  // .resolveTable member).
  def resolveTable(dbAddress: String, table: String, properties: Properties): StructType = {
    JDBCRDD.resolveTable(dbAddress, table, properties)
  }
}
