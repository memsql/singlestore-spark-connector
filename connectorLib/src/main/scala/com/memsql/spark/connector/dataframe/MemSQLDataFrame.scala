package com.memsql.spark.connector.dataframe

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Types}

import scala.reflect.ClassTag

import org.apache.spark.{Logging, Partition, SparkContext, SparkException, TaskContext, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import com.memsql.spark.connector.util.NextIterator

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import com.memsql.spark.connector.rdd.MemSQLRDD
import org.apache.commons.lang.StringEscapeUtils

object MemSQLDataFrameUtils {
  def DataFrameTypeToJDBCType(dataType : DataType) : Int = {
    dataType match {
      case IntegerType => java.sql.Types.INTEGER
      case LongType => java.sql.Types.BIGINT
      case DoubleType => java.sql.Types.DOUBLE
      case FloatType => java.sql.Types.REAL
      case ShortType => java.sql.Types.INTEGER
      case ByteType => java.sql.Types.INTEGER
      case BooleanType => java.sql.Types.BIT
      case StringType => java.sql.Types.CLOB
      case BinaryType => java.sql.Types.BLOB
      case TimestampType => java.sql.Types.TIMESTAMP
      case DateType => java.sql.Types.DATE
      case DecimalType.Unlimited => java.sql.Types.DECIMAL
      case _ => throw new IllegalArgumentException("Can't translate type " + dataType.toString)
    }
  }

  def JDBCTypeToDataFrameType(dataType : Int) : DataType = {
    dataType match {
      case java.sql.Types.INTEGER => IntegerType 
      case java.sql.Types.BIGINT => IntegerType// LongType  // TODO: This can overflow if the bigint is truely big, but probably fine for now
      case java.sql.Types.DOUBLE => DoubleType 
      case java.sql.Types.REAL => FloatType 
      case java.sql.Types.BIT => BooleanType 
      case java.sql.Types.CLOB => StringType 
      case java.sql.Types.BLOB => BinaryType 
      case java.sql.Types.TIMESTAMP => TimestampType 
      case java.sql.Types.DATE => DateType 
      case java.sql.Types.DECIMAL => DecimalType.Unlimited 
      case java.sql.Types.LONGNVARCHAR => StringType
      case java.sql.Types.LONGVARCHAR => StringType
      case java.sql.Types.VARCHAR => StringType
      case java.sql.Types.NVARCHAR => StringType
      case _ => throw new IllegalArgumentException("Can't translate type " + dataType.toString)
    }
  }
  
  def GetJDBCValue(dataType : Int, ix : Int, row : ResultSet) : Any = {
    dataType match {
      case java.sql.Types.INTEGER => row.getInt(ix)
      case java.sql.Types.BIGINT => row.getInt(ix)
      case java.sql.Types.DOUBLE => row.getDouble(ix) 
      case java.sql.Types.REAL => row.getDouble(ix) 
      case java.sql.Types.BIT => row.getBoolean(ix) 
      case java.sql.Types.CLOB => row.getString(ix)  
      case java.sql.Types.BLOB => row.getClob(ix) 
      case java.sql.Types.TIMESTAMP => row.getString(ix) 
      case java.sql.Types.DATE => row.getDate(ix) 
      case java.sql.Types.DECIMAL => row.getBigDecimal(ix) 
      case java.sql.Types.LONGNVARCHAR => row.getString(ix) 
      case java.sql.Types.LONGVARCHAR => row.getString(ix) 
      case java.sql.Types.VARCHAR => row.getString(ix) 
      case java.sql.Types.NVARCHAR => row.getString(ix) 
      case _ => throw new IllegalArgumentException("Can't translate type " + dataType.toString)
    }
  }
}

object MemSQLDataFrame {
  def MakeMemSQLRowRDD(
    sc: SparkContext,
    dbHost: String,
    dbPort: Int,
    user: String,
    password: String,
    dbName: String,
    query: String) : MemSQLRDD[Row] = {
    
    return new MemSQLRDD(sc,
			 dbHost, 
			 dbPort, 
			 user, 
			 password, 
			 dbName, 
			 query, 
  			 (r:ResultSet) => {
  			   val count = r.getMetaData.getColumnCount
  			   Row.fromSeq(Range(0,count)
                                       .map(i => MemSQLDataFrameUtils.GetJDBCValue(r.getMetaData.getColumnType(i+1), i+1, r)))			  
  			 })
  }

  def MakeMemSQLDF(
    sqlContext: SQLContext,
    dbHost: String,
    dbPort: Int,
    user: String,
    password: String,
    dbName: String,
    tableName: String) : DataFrame = {
    sqlContext.load("com.memsql.spark.connector.dataframe.MemSQLRelationProvider", Map(
      "dbHost" -> dbHost,
      "dbPort" -> dbPort.toString,
      "user" -> user,
      "password" -> password,
      "dbName" -> dbName,
      "tableName" -> tableName))
  }

  def getQuerySchema(
    dbHost: String,
    dbPort: Int,
    user: String,
    password: String,
    dbName: String,
    query: String) : StructType = {

    // Prepare the MySQL JDBC driver.
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val conn = MemSQLRDD.getConnection(dbHost, dbPort, user, password, dbName)  
    val versionStmt = conn.createStatement
    val metadata = versionStmt.executeQuery(limitZero(query)).getMetaData
    val count = metadata.getColumnCount
    val schema = StructType(Range(0,count).map(i => StructField(metadata.getColumnName(i+1), 
                                                                MemSQLDataFrameUtils.JDBCTypeToDataFrameType(metadata.getColumnType(i+1)), 
                                                                metadata.isNullable(i+1) == ResultSetMetaData.columnNullable)))
    return schema
  }

  def limitZero(q: String) : String = {
    return "SELECT * FROM (" + q + ") tab_alias LIMIT 0"
  }

}

case class MemSQLScan(@transient val rdd: MemSQLRDD[Row], @transient val sqlContext: SQLContext) 
   extends BaseRelation with PrunedFilteredScan
{
  val schema : StructType = MemSQLDataFrame.getQuerySchema(rdd.dbHost, rdd.dbPort, rdd.user, rdd.password, rdd.dbName, rdd.sql)

  private def getWhere(filters: Array[Filter]) : String = {
    var result = ""
    for (i <- 0 to (filters.size - 1))
    {
      if (i != 0) // scala apparently has no "pythonic" join
      {
	result = result + " and "
      }
      result  = result + (filters(i) match
      {
	case EqualTo(attr, v) =>  attr + " = '" + StringEscapeUtils.escapeSql(v.toString) + "'"
	case GreaterThan(attr, v) =>  attr + " > '" + StringEscapeUtils.escapeSql(v.toString) + "'"
	case LessThan(attr, v) =>  attr + " < '" + StringEscapeUtils.escapeSql(v.toString) + "'"
	case GreaterThanOrEqual(attr, v) => attr + " >= '" + StringEscapeUtils.escapeSql(v.toString) + "'"
	case LessThanOrEqual(attr, v) => attr + " <= '" + StringEscapeUtils.escapeSql(v.toString) + "'"
	case In(attr, vs) => {
          var inlist = attr + " in (" 
          for (j <- 0 to (vs.size - 1))
          {
            if (j != 0)
            {
              inlist += ","
            }
            inlist = inlist + "'" + StringEscapeUtils.escapeSql(vs(j).toString) + "'"
          }
          inlist + " )"
        }
      })
    }
    return result
  }
  private def getProject(requiredColumns: Array[String]): String = {
    if (requiredColumns.size == 0) // for df.count, df.is_empty
    {
      return "1" 
    }
    var result = ""
    for (i <- 0 to (requiredColumns.size - 1))
    {
      if (i != 0)
      {
	result = result + ", "
      } 
      result = result + requiredColumns(i)
    }
    return result
  }
  
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    var sql = "SELECT " + getProject(requiredColumns) + " FROM (" + rdd.sql + ") tab_alias"
    if (filters.size != 0)
    {
      sql = sql + " WHERE " + getWhere(filters)
    }
    return MemSQLDataFrame.MakeMemSQLRowRDD(sqlContext.sparkContext, rdd.dbHost, rdd.dbPort, rdd.user, rdd.password, rdd.dbName, sql)
  }
}

class MemSQLRelationProvider extends RelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String,String]) : MemSQLScan = {
    return new MemSQLScan(MemSQLDataFrame.MakeMemSQLRowRDD(sqlContext.sparkContext,
						           parameters("dbHost"),
						           parameters("dbPort").toInt,
						           parameters("user"),
						           parameters("password"),
						           parameters("dbName"),
						           "SELECT * FROM " + parameters("tableName")),
			  sqlContext)
  }
}
