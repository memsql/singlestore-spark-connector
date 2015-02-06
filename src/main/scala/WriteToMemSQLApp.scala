import java.sql.{DriverManager, ResultSet}

import com.memsql.spark.connector._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WriteToMemSQLApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Write to MemSQL Application")
    val sc = new SparkContext(conf)

    val host = "127.0.0.1"
    val port = 3306
    val dbName = "memsqlrdd_db"
    val user = "root"
    val password = ""
    val outputTableName = "output"

    val dbAddress = "jdbc:mysql://" + host + ":" + port + "/" + dbName
    val conn = DriverManager.getConnection(dbAddress, user, password)
    val stmt = conn.createStatement
    stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName)
    stmt.execute("USE " + dbName)
    stmt.execute("DROP TABLE IF EXISTS output")
    stmt.execute("""
       CREATE TABLE output
       (data VARCHAR(200), SHARD KEY (data))
    """)
    stmt.close()

    var values = Array[Array[String]]()
    for (i <- 0 until 1000) {
      values = values :+ Array("test_data_" + "%04d".format(i))
    }

    val rdd = sc.parallelize(values)
    rdd.saveToMemsql(host, port, user, password, dbName, outputTableName)
  }
}
