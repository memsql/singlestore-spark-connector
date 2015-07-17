package test

import com.memsql.spark.context.MemSQLSparkContext
import java.sql.{DriverManager, ResultSet}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import com.memsql.spark.connector.dataframe.MemSQLDataFrame
import com.memsql.spark.connector.rdd.MemSQLRDD
import com.memsql.spark.connector._

// assume a distributed.py cluster with three aggs and two leaves
//
object TestMemSQLContextVeryBasic 
{
    def main(args: Array[String]) 
    {
        val conf = new SparkConf().setAppName("TestMemSQLContextVeryBasic")
        val sc = new MemSQLSparkContext(conf, "127.0.0.1", 10000, "root", "")
        val sqlContext = new SQLContext(sc)
        TestUtils.DropAndCreate("db")
      
        assert(sc.GetMemSQLNodesAvailableForIngest.size == 2)
        assert(sc.GetMemSQLNodesAvailableForIngest(0)._1 == "127.0.0.1")
        assert(sc.GetMemSQLNodesAvailableForIngest(1)._1 == "127.0.0.1")
        assert(sc.GetMemSQLNodesAvailableForIngest(0)._2 == 10003)
        assert(sc.GetMemSQLNodesAvailableForIngest(1)._2 == 10004)
    
        assert(sc.GetMemSQLLeaves.size == 2)
        assert(sc.GetMemSQLLeaves(0)._1 == "127.0.0.1")
        assert(sc.GetMemSQLLeaves(1)._1 == "127.0.0.1")
        assert(sc.GetMemSQLLeaves(0)._2 == 10001)
        assert(sc.GetMemSQLLeaves(1)._2 == 10002)

        val rdd = sc.parallelize(
          Array(Row(1,"pieguy"),
                Row(2,"gbop"),
                Row(3,"berrydave"),
                Row(4,"psyduck"),
                Row(null,"null")))
        val schema = StructType(Array(StructField("a",IntegerType,true),
                                      StructField("b",StringType,false)))
        val df = sqlContext.createDataFrame(rdd, schema)
      
        val memdf =  df.createMemSQLTableAs("db","t")
        assert(TestUtils.EqualDFs(df, memdf))
        val memdf2 = df.createMemSQLTableAs("db","t2","127.0.0.1",10000,"root","")
        assert(TestUtils.EqualDFs(df, memdf2))
    }
}
