package com.memsql.spark.connector

import org.apache.spark.SparkContext

/**
 * A MemSQL cluster aware [[org.apache.spark.sql.SQLContext]] that can read and write
 * [[org.apache.spark.sql.DataFrame]]s from MemSQL tables.
 *
 * Configuration for the MemSQL cluster is set via [[com.memsql.spark.connector.MemSQLConf]].
 *
 * NOTE: This class is just a proxy so you can import from
 * com.memsql.spark.connector rather than org.apache.spark.sql.memsql
 * @see [[org.apache.spark.sql.memsql.MemSQLContext]]
 *
 * @param sc The context for your Spark application
 */
class MemSQLContext(sc: SparkContext) extends org.apache.spark.sql.memsql.MemSQLContext(sc)
