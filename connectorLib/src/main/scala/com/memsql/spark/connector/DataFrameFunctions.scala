package com.memsql.spark.connector

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

import com.memsql.spark.connector.rdd.MemSQLRDD
import com.memsql.spark.connector.dataframe.MemSQLDataFrameUtils

import scala.reflect.ClassTag
import org.apache.spark.{SparkException, Logging}

class DataFrameFunctions(df: DataFrame) extends Serializable with Logging 
{
    def saveToMemSQL(dbHost: String,
                     dbPort: Int,
                     user: String,
                     password: String,
                     dbName: String,
                     tableName: String,
                     onDuplicateKeySql: String = "",
                     useInsertIgnore: Boolean = false,
                     insertBatchSize: Int = 10000) 
    {
        val insertTable = new StringBuilder()
        insertTable.append(tableName).append("(")
        var first = true
        for (col <- df.schema)
        {
            if (!first)
            {
                insertTable.append(", ")
            }
            first = false;
            insertTable.append(col.name)
        }
        val insertTableString = insertTable.append(")").toString
        df.rdd.saveToMemSQL(dbHost, dbPort, user, password, dbName, insertTableString, onDuplicateKeySql, useInsertIgnore, insertBatchSize)
    }

    def createMemSQLTableAs(dbHost: String,
                            dbPort: Int,
                            user: String,
                            password: String,
                            dbName: String,
                            tableName: String,
                            ifNotExists: Boolean = false) 
    {
        val sql = new StringBuilder()
        sql.append("CREATE TABLE ")
        if (ifNotExists)
        {
            sql.append("IF NOT EXISTS ")
        }
        sql.append(tableName).append(" (")
        for (col <- df.schema)
        {
            sql.append(col.name).append(" ")
            sql.append(MemSQLDataFrameUtils.DataFrameTypeToMemSQLTypeString(col.dataType))
            if (!col.nullable)
            {
                sql.append(" NOT NULL")
            }
            sql.append(",")
        }
        sql.append("SHARD())") // not always optimial, but in the spirit of user not having to make hard choices

        val conn = MemSQLRDD.getConnection(dbHost, dbPort, user, password, dbName)
        val stmt = conn.createStatement
        stmt.executeUpdate(sql.toString) // TODO: should I be handling errors, or just expect the caller to catch them...
        stmt.close()

        saveToMemSQL(dbHost, dbPort, user, password, dbName, tableName)
    }
}

