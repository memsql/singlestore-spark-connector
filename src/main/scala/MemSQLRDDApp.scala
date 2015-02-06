import java.sql.{DriverManager, ResultSet}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.memsql.spark.connector.rdd.MemSQLRDD

object MemSQLRDDApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MemSQLRDD Application")
    val sc = new SparkContext(conf)

    val host = "127.0.0.1"
    val port = 3306
    val user = "root"
    val password = ""
    val dbName = "memsqlrdd_db"

    val dbAddress = "jdbc:mysql://" + host + ":" + port
    val conn = DriverManager.getConnection(dbAddress, user, password)
    val stmt = conn.createStatement
    stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName)
    stmt.execute("USE " + dbName)
    stmt.execute("DROP TABLE IF EXISTS x")
    stmt.execute("""
       CREATE TABLE x
       (id INTEGER PRIMARY KEY, data VARCHAR(200))
    """)
    var insertQuery = "INSERT INTO x VALUES "
    // Insert a bunch of rows like (1, "test_data_0001").
    for (i <- 0 until 999) {
      insertQuery = insertQuery + "(" + i + ", 'test_data_" + "%04d".format(i) + "'),"
    }
    insertQuery = insertQuery + "(" + 999 + ", 'test_data_" + "%04d".format(999) + "')"
    stmt.execute(insertQuery)
    stmt.close()

    val rdd = new MemSQLRDD(
      sc,
      host,
      port,
      user,
      password,
      dbName,
      "SELECT * FROM x",
      (r: ResultSet) => { r.getString(2) })
    rdd.saveAsTextFile("memsql_read_rdd_output.txt")
  }
}
