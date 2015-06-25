package test

import test.MemSQLTestSetup

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

import com.memsql.spark.connector.dataframe.MemSQLDataFrame
import com.memsql.spark.connector.rdd.MemSQLRDD

object TestMemSQLDataFrameVeryBasic {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MemSQLRDD Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val host = "127.0.0.1"
    val port = 3306
    val user = "root"
    val password = ""
    val dbName = "x_db"
    
    MemSQLTestSetup.SetupBasic()

    val df_t = MemSQLDataFrame.MakeMemSQLDF(
      sqlContext,
      host,
      port,
      user,
      password,
      dbName,
      "t")
    val df_s = MemSQLDataFrame.MakeMemSQLDF(
      sqlContext,
      host,
      port,
      user,
      password,
      dbName,
      "s")
    val df_r = MemSQLDataFrame.MakeMemSQLDF(
      sqlContext,
      host,
      port,
      user,
      password,
      dbName,
      "r")

    val dfs = Array(df_t, df_s)//, df_r) TODO: reference tables dont work
    for (i <- 0 until (dfs.size - 1))
    {

        // TODO: We dont automatically test that anything is actually pushed down
        // but you can see them being pushed down by reading the memsql tracelog
      
        println(dfs(i).schema)
        println(dfs(i).rdd.toDebugString)
        var results = dfs(i).collect()
        println(results.size)
        assert(results.size == 1000)
        assert(dfs(i).count() == 1000)
    
        assert(dfs(i).filter(dfs(i)("id") === 1).collect().size == 1)
        assert(dfs(i).filter(dfs(i)("id") === 1).count == 1)
        
        assert(dfs(i).filter(dfs(i)("id") <= 1).collect().size == 2)
        assert(dfs(i).filter(dfs(i)("id") <= 1).count == 2)
    
        assert(dfs(i).filter(dfs(i)("id") < 1).collect().size == 1)
        assert(dfs(i).filter(dfs(i)("id") < 1).count == 1)
    
        assert(dfs(i).filter(dfs(i)("id") > 1).collect().size == 998)
        assert(dfs(i).filter(dfs(i)("id") > 1).count == 998)
    
        assert(dfs(i).filter(dfs(i)("id") >= 1).collect().size == 999)
        assert(dfs(i).filter(dfs(i)("id") >= 1).count == 999)
    
      // TODO: Get inlists to work
      //        assert(dfs(i).filter(dfs(i)("id").in(1,2,3,-1)).collect().size == 3)
      //        assert(dfs(i).filter(dfs(i)("id").in(1,2,3,-1)).count == 3)
    
        assert(dfs(i).filter(dfs(i)("data") === "test_data_0000").collect().size == 1)
        assert(dfs(i).filter(dfs(i)("data") === "test_data_0000").count() == 1)
    
        // assert(dfs(i).filter(dfs(i)("data").in("test_data_0000","test_data_0000","not_present")).collect().size == 2)
        // assert(dfs(i).filter(dfs(i)("data").in("test_data_0000","test_data_0000","not_present")).count() == 2)

    }
  }
}
