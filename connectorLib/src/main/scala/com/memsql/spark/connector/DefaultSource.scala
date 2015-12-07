package com.memsql.spark.connector

/**
  * This class is a proxy for the actual implementation in org.apache.spark.
  * It allows you to write data to MemSQL via the Spark RelationProvider API.
  *
  * Example:
  *   df.write.format("com.memsql.spark.connector").save("foo.bar")
  */
class DefaultSource extends org.apache.spark.sql.memsql.DefaultSource
