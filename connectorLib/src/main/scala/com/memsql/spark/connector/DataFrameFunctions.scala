package com.memsql.spark.connector

import java.sql._

import com.memsql.spark.context.MemSQLContext
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame

import com.memsql.spark.connector.rdd._
import com.memsql.spark.connector.dataframe._

class DataFrameFunctions(df: DataFrame) extends Serializable {
  /*
   * Saves a Spark dataframe to a memsql table with the same column names.
   * If dbHost, dbPort, user and password are not specified,
   * the MemSQLContext will determine where each partition's data is sent.
   * Otherwise, all partitions will load into the node specified by MemSQLContext
   */
  def saveToMemSQL(dbName: String,
                   tableName: String,
                   dbHost: String = null,
                   dbPort: Int = -1,
                   user: String = null,
                   password: String = null,
                   onDuplicateKeySql: String = "",
                   useInsertIgnore: Boolean = false,
                   upsertBatchSize: Int = RDDFunctions.DEFAULT_UPSERT_BATCH_SIZE,
                   useKeylessShardedOptimization: Boolean = false): Long = {
    val (theHost, thePort, theUser, thePassword) = getMemSQLCredentials(dbHost, dbPort, user, password)

    val insertTable = new StringBuilder()
    insertTable.append(tableName).append("(")
    var first = true
    for (col <- df.schema) {
      if (!first) {
        insertTable.append(", ")
      }
      first = false
      insertTable.append("`").append(col.name).append("`")
    }
    val insertTableString = insertTable.append(")").toString

    df.rdd.saveToMemSQL(dbName, insertTableString, theHost, thePort, theUser, thePassword, onDuplicateKeySql,
                        useInsertIgnore, upsertBatchSize, useKeylessShardedOptimization)
  }

  /*
   * Creates a MemSQL table with the schema matching the Spark dataframe and loads the data into it.
   * If dbHost, dbPort, user and password are not specified,
   * the MemSQLContext will determine where each partition's data is sent.
   * Otherwise, all partitions will load into the node specified by MemSQLContext
   */
  def createMemSQLTableAs(dbName: String,
                          tableName: String,
                          dbHost: String = null,
                          dbPort: Int = -1,
                          user: String = null,
                          password: String = null,
                          ifNotExists: Boolean = false,
                          keys: List[MemSQLKey] = List(),
                          extraCols: List[MemSQLExtraColumn] = List(),
                          useKeylessShardedOptimization: Boolean = false): DataFrame = {
    val resultDf = df.createMemSQLTableFromSchema(dbName, tableName, dbHost, dbPort, user, password, ifNotExists, keys, extraCols)
    df.saveToMemSQL(dbName, tableName, dbHost, dbPort, user, password, useKeylessShardedOptimization = useKeylessShardedOptimization)
    resultDf
  }

  def createMemSQLTableFromSchema(dbName: String,
                                  tableName: String,
                                  dbHost: String = null,
                                  dbPort: Int = -1,
                                  user: String = null,
                                  password: String = null,
                                  ifNotExists: Boolean = false,
                                  keys: List[MemSQLKey] = List(),
                                  extraCols: List[MemSQLExtraColumn] = List()): DataFrame = {
    val (theHost, thePort, theUser, thePassword) = getMemSQLCredentials(dbHost, dbPort, user, password)
    val sql = new StringBuilder()
    sql.append("CREATE ")
    sql.append("TABLE ")
    if (ifNotExists) {
      sql.append("IF NOT EXISTS ")
    }
    sql.append(s"`$tableName`").append(" (")
    for (col <- df.schema) {
      sql.append("`").append(col.name).append("` ")
      sql.append(MemSQLDataFrameUtils.DataFrameTypeToMemSQLTypeString(col.dataType))

      if (col.nullable) {
        sql.append(" NULL DEFAULT NULL")
      } else {
        sql.append(" NOT NULL")
      }
      sql.append(",")
    }
    val hasShardKey = keys.exists(_.canBeUsedAsShardKey)
    val theKeys = if (hasShardKey) keys else keys :+ Shard()
    sql.append(theKeys.map((k: MemSQLKey) => k.toSQL).mkString(","))
    if (extraCols.length > 0) {
      sql.append(",")
    }
    sql.append(extraCols.map((k: MemSQLExtraColumn) => k.toSQL).mkString(","))
    sql.append(")")

    var conn: Connection = null
    var stmt: Statement = null
    try {
      conn = MemSQLRDD.getConnection(theHost, thePort, theUser, thePassword)
      stmt = conn.createStatement
      stmt.execute(s"CREATE DATABASE IF NOT EXISTS `$dbName`")
      stmt.execute(s"USE `$dbName`")
      stmt.executeUpdate(sql.toString) // TODO: should I be handling errors, or just expect the caller to catch them...
    } finally {
      if (stmt != null && !stmt.isClosed()) {
        stmt.close()
      }
      if (null != conn && !conn.isClosed()) {
        conn.close()
      }
    }
    MemSQLDataFrame.MakeMemSQLDF(df.sqlContext, theHost, thePort, theUser, thePassword, dbName, "SELECT * FROM " + tableName)
  }

  private def getMemSQLCredentials(dbHost: String, dbPort: Int, user: String, password: String): (String, Int, String, String) = {
    dbHost == null || dbPort == -1 || user == null || password == null match {
      case true => {
        df.sqlContext match {
          case memsqlContext: MemSQLContext => {
            (memsqlContext.getMemSQLMasterAggregator.host,
              memsqlContext.getMemSQLMasterAggregator.port,
              memsqlContext.getMemSQLUserName,
              memsqlContext.getMemSQLPassword)
          }
          case default => throw new SparkException("saveToMemSQL requires creating the DataFrame with a MemSQLContext " +
            "or explicitly setting dbName, dbHost, user, and password")
        }
      }
      case false => (dbHost, dbPort, user, password)
    }
  }
}
