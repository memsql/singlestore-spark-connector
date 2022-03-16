package com.singlestore.spark

import com.singlestore.spark.JdbcHelpers.{
  executeQuery,
  getClusterConnProperties,
  getAdminConnProperties
}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils

object SQLHelper extends LazyLogging {
  implicit class QueryMethods(spark: SparkSession) {
    private def singlestoreQuery(db: Option[String],
                                 query: String,
                                 queryAdmin: Boolean,
                                 variables: Any*): Iterator[Row] = {
      val ctx = spark.sqlContext
      var opts = ctx.getAllConfs.collect {
        case (k, v) if k.startsWith(DefaultSource.SINGLESTORE_GLOBAL_OPTION_PREFIX) =>
          k.stripPrefix(DefaultSource.SINGLESTORE_GLOBAL_OPTION_PREFIX) -> v
        case (k, v) if k.startsWith(DefaultSource.MEMSQL_GLOBAL_OPTION_PREFIX) =>
          k.stripPrefix(DefaultSource.MEMSQL_GLOBAL_OPTION_PREFIX) -> v
      }

      if (db.isDefined) {
        val dbValue = db.get
        if (dbValue.isEmpty) {
          opts -= "database"
        } else {
          opts += ("database" -> dbValue)
        }
      }

      val conf = SinglestoreOptions(CaseInsensitiveMap(opts))

      val conn = if (queryAdmin) {
        val properties = getAdminConnProperties(conf, isOnExecutor = false) match {
          case Some(properties) => properties
          case None =>
            throw new IllegalArgumentException(
              "adminEndpoint option is required in order to perform this operation")
        }
        SinglestoreConnectionPool.getConnection(properties)
      } else {
        SinglestoreConnectionPool.getConnection(
          getClusterConnProperties(conf, isOnExecutor = false))
      }

      try {
        executeQuery(conn, query, variables: _*)
      } finally {
        conn.close()
      }
    }

    def executeSinglestoreQueryDB(db: String, query: String, variables: Any*): Iterator[Row] = {
      singlestoreQuery(Some(db), query, false, variables: _*)
    }

    def executeSinglestoreQuery(query: String, variables: Any*): Iterator[Row] = {
      singlestoreQuery(None, query, false, variables: _*)
    }

    def executeSinglestoreAdminQueryDB(db: String,
                                       query: String,
                                       variables: Any*): Iterator[Row] = {
      singlestoreQuery(Some(db), query, true, variables: _*)
    }

    def executeSinglestoreAdminQuery(query: String, variables: Any*): Iterator[Row] = {
      singlestoreQuery(None, query, true, variables: _*)
    }

    @Deprecated def executeMemsqlQueryDB(db: String,
                                         query: String,
                                         variables: Any*): Iterator[Row] = {
      singlestoreQuery(Some(db), query, false, variables: _*)
    }

    @Deprecated def executeMemsqlQuery(query: String, variables: Any*): Iterator[Row] = {
      singlestoreQuery(None, query, false, variables: _*)
    }

    @Deprecated def executeMemsqlAdminQueryDB(db: String,
                                              query: String,
                                              variables: Any*): Iterator[Row] = {
      singlestoreQuery(Some(db), query, true, variables: _*)
    }

    @Deprecated def executeMemsqlAdminQuery(query: String, variables: Any*): Iterator[Row] = {
      singlestoreQuery(None, query, true, variables: _*)
    }
  }

  def executeSinglestoreQueryDB(spark: SparkSession,
                                db: String,
                                query: String,
                                variables: Any*): Iterator[Row] = {
    spark.executeSinglestoreQueryDB(db, query, variables: _*)
  }

  def executeSinglestoreQuery(spark: SparkSession,
                              query: String,
                              variables: Any*): Iterator[Row] = {
    spark.executeSinglestoreQuery(query, variables: _*)
  }

  def executeSinglestoreAdminQueryDB(spark: SparkSession,
                                     db: String,
                                     query: String,
                                     variables: Any*): Iterator[Row] = {
    spark.executeSinglestoreAdminQueryDB(db, query, variables: _*)
  }

  def executeSinglestoreAdminQuery(spark: SparkSession,
                                   query: String,
                                   variables: Any*): Iterator[Row] = {
    spark.executeSinglestoreAdminQuery(query, variables: _*)
  }

  @Deprecated def executeMemsqlQueryDB(spark: SparkSession,
                                       db: String,
                                       query: String,
                                       variables: Any*): Iterator[Row] = {
    spark.executeSinglestoreQueryDB(db, query, variables: _*)
  }

  @Deprecated def executeMemsqlQuery(spark: SparkSession,
                                     query: String,
                                     variables: Any*): Iterator[Row] = {
    spark.executeSinglestoreQuery(query, variables: _*)
  }

  @Deprecated def executeMemsqlAdminQueryDB(spark: SparkSession,
                                            db: String,
                                            query: String,
                                            variables: Any*): Iterator[Row] = {
    spark.executeSinglestoreAdminQueryDB(db, query, variables: _*)
  }

  @Deprecated def executeMemsqlAdminQuery(spark: SparkSession,
                                          query: String,
                                          variables: Any*): Iterator[Row] = {
    spark.executeSinglestoreAdminQuery(query, variables: _*)
  }
}
