package com.memsql.spark.connector

import org.apache.spark.SparkContext

/**
  * This class is just a proxy so you can import from
  * com.memsql.spark.connector rather than org.apache.spark.sql.memsql
  * @see [[org.apache.spark.sql.memsql.MemSQLContext]]
  */
class MemSQLContext(sc: SparkContext) extends org.apache.spark.sql.memsql.MemSQLContext(sc)
