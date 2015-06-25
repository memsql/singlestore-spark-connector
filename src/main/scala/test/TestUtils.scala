package test
import java.sql.{DriverManager, ResultSet}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

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
       (id BIGINT PRIMARY KEY, data VARCHAR(200), key(data))
    """)
    stmt.execute("""
       CREATE TABLE s 
       (id BIGINT , data VARCHAR(200), key(id), key(data), shard())
    """)
    stmt.execute("""
       CREATE reference TABLE r
       (id BIGINT PRIMARY KEY, data VARCHAR(200), key(data))
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

}
