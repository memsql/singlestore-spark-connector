package com.memsql.spark

import com.memsql.spark.JdbcHelpers.{executeQuery, getDDLJDBCOptions}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils

object SQLHelper extends LazyLogging {
  implicit class QueryMethods(spark: SparkSession) {
    private def memsqlQuery(db: Option[String], query: String, variables: Any*): Iterator[Row] = {
      val ctx = spark.sqlContext
      var opts =
        for ((k, v) <- ctx.getAllConfs if k.startsWith(DefaultSource.MEMSQL_GLOBAL_OPTION_PREFIX))
          yield k.stripPrefix(DefaultSource.MEMSQL_GLOBAL_OPTION_PREFIX) -> v

      if (db.isDefined) {
        val dbValue = db.get
        if (dbValue.isEmpty) {
          opts -= "database"
        } else {
          opts += ("database" -> dbValue)
        }
      }

      val conf = MemsqlOptions(CaseInsensitiveMap(opts))
      val conn = JdbcUtils.createConnectionFactory(getDDLJDBCOptions(conf))()
      try {
        executeQuery(conn, query, variables: _*)
      } finally {
        conn.close()
      }
    }

    def executeMemsqlQueryDB(db: String, query: String, variables: Any*): Iterator[Row] = {
      memsqlQuery(Some(db), query, variables: _*)
    }

    def executeMemsqlQuery(query: String, variables: Any*): Iterator[Row] = {
      memsqlQuery(None, query, variables: _*)
    }
  }

  def executeMemsqlQueryDB(spark: SparkSession,
                           db: String,
                           query: String,
                           variables: Any*): Iterator[Row] = {
    spark.executeMemsqlQueryDB(db, query, variables: _*)
  }

  def executeMemsqlQuery(spark: SparkSession, query: String, variables: Any*): Iterator[Row] = {
    spark.executeMemsqlQuery(query, variables: _*)
  }
}
