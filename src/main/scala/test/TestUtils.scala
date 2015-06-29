package test
import java.sql.{DriverManager, ResultSet}
import org.apache.spark.sql.catalyst.expressions.RowOrdering

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import com.memsql.spark.connector.dataframe.MemSQLDataFrame
import org.apache.spark.sql._

object MemSQLTestSetup {
  def SetupBasic() {
    val host = "127.0.0.1"
    val port = 3306
    val user = "root"
    val password = ""
    val dbName = "x_db"

    val dbAddress = "jdbc:mysql://" + host + ":" + port
    val conn = DriverManager.getConnection(dbAddress, user, password)
    val stmt = conn.createStatement
    stmt.execute("DROP DATABASE IF EXISTS " + dbName)
    stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName)
    stmt.execute("USE " + dbName)
    stmt.execute("""
       CREATE TABLE t 
       (id INT PRIMARY KEY, data VARCHAR(200), key(data))
    """)
    stmt.execute("""
       CREATE TABLE s 
       (id INT , data VARCHAR(200), key(id), key(data), shard())
    """)
    stmt.execute("""
       CREATE reference TABLE r
       (id INT PRIMARY KEY, data VARCHAR(200), key(data))
    """)

    var insertQuery = ""
    // Insert a bunch of rows like (1, "test_data_0001").
    for (i <- 0 until 999) {
      insertQuery = insertQuery + "(" + i + ", 'test_data_" + "%04d".format(i) + "'),"
    }
    insertQuery = insertQuery + "(" + 999 + ", 'test_data_" + "%04d".format(999) + "')"
    stmt.execute("INSERT INTO t values" + insertQuery)
    stmt.execute("INSERT INTO s values" + insertQuery)
    stmt.execute("INSERT INTO r values" + insertQuery)
    stmt.close()
  }

  def SetupAllMemSQLTypes(sqlContext: SQLContext, nullable: Boolean): DataFrame = {
    val host = "127.0.0.1"
    val port = 3306
    val user = "root"
    val password = ""
    val dbName = "alltypes_db"

    val dbAddress = "jdbc:mysql://" + host + ":" + port
    val conn = DriverManager.getConnection(dbAddress, user, password)
    val stmt = conn.createStatement

    stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName)
    stmt.execute("use " + dbName)
    val tbname = if (nullable) "alltypes_nullable" else "alltypes_not_null"
    stmt.execute("drop table if exists " + tbname)

    val create = "create table " + tbname + " (" + Types.MemSQLTypes.map(_._1).map((t:String) => Types.ToCol(t)
                                                                                   + " " + t + (if (!nullable) " not null" else " null default null")).mkString(",") + ",shard())"
    stmt.execute(create)
    
    var insertQuery = "insert into " + tbname + " values"
    for (i <- 0 until 3)
    {
      insertQuery = insertQuery + "(" + Types.MemSQLTypes.map("'" + _._2(i) + "'").mkString(",") + ")"
      if (i < 2)
      {
        insertQuery = insertQuery + ","
      }
    }
    if (nullable)
    {
      insertQuery = insertQuery + ", (" + Types.MemSQLTypes.map((a:Any) => "null").mkString(",") + ")"
    }
    stmt.execute(insertQuery)
    return TestUtils.MemSQLDF(sqlContext, dbName, tbname)
  }
  
}
object TestUtils {
  def DropAndCreate(dbName: String) {
    val host = "127.0.0.1"
    val port = 3306
    val user = "root"
    val password = ""
    val dbAddress = "jdbc:mysql://" + host + ":" + port
    
    val conn = DriverManager.getConnection(dbAddress, user, password)
    val stmt = conn.createStatement
    stmt.execute("DROP DATABASE IF EXISTS " + dbName)
    stmt.execute("CREATE DATABASE " + dbName)
    stmt.close()
  }
  def MemSQLDF(sqlContext : SQLContext, dbName : String, tableName : String) : DataFrame = {
    MemSQLDataFrame.MakeMemSQLDF(
      sqlContext,
      "127.0.0.1",
      3306,
      "root",
      "",
      dbName,
      tableName)
  }
  def CollectAndSort(df: DataFrame): Seq[Row] = {
    return df.collect.sorted(RowOrdering.forSchema(df.schema.map(_.dataType))) // zomg why is this like this?
  }
  def EqualDFs(df1: DataFrame, df2: DataFrame): Boolean = {
    return CollectAndSort(df1).equals(CollectAndSort(df2))
  }
}
object Types {
  val MemSQLTypes: Array[(String,Array[String])] = Array( // not a complete list of course
     ("int", Array("1","2","3")),
    ("bigint",Array("4","5","6")),
    ("tinyint",Array("7","8","9")),
    ("text",Array("a","b","c")),
    ("blob",Array("e","f","g")),
    ("varchar(100)",Array("do","rae","me")),
    ("varbinary(100)",Array("one","two","three")),
    ("decimal(5,1)",Array("1.1","2.2","3.3")),
    ("double",Array("4.4","5.5","6.6")),
    ("float",Array("7.7","8.8","9.9")),
    ("datetime",Array("1990-08-23 01:01:01.0","1990-08-23 01:01:02.0","1990-08-23 01:01:03.0")),
    ("timestamp",Array("1990-08-23 01:01:04.0","1990-08-23 01:01:05.0","1990-08-23 01:01:06.0")),
    ("date",Array("1990-08-23","1990-23-09","1990-23-10")),
    ("time",Array("03:14:15.0","03:14:16.0","03:14:17.0")))
  def ToCol(tp: String): String = "val_" + tp.replace("(","_").replace(")","").replace(",","_")
  val SparkSQLTypes: Array[DataType] = Array(
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    ShortType,
    ByteType,
    BooleanType,
    StringType,
    BinaryType,
    TimestampType,
    DateType)
}
