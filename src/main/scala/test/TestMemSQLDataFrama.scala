package test

import com.memsql.spark.context.MemSQLSparkContext
import java.sql.{DriverManager, ResultSet}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import com.memsql.spark.connector.dataframe._
import com.memsql.spark.connector.rdd.MemSQLRDD
import com.memsql.spark.connector._


object TestMemSQLDataFrameVeryBasic {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MemSQLRDD Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val host = "127.0.0.1"
    val port = 10000
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
      "SELECT * FROM t")
    val df_s = MemSQLDataFrame.MakeMemSQLDF(
      sqlContext,
      host,
      port,
      user,
      password,
      dbName,
      "SELECT * FROM s")
    val df_r = MemSQLDataFrame.MakeMemSQLDF(
      sqlContext,
      host,
      port,
      user,
      password,
      dbName,
      "SELECT * FROM r")

    // we want to make sure that we pushdown simple queries to the leaves
    assert (df_t.rdd.partitions.size > 1)
    assert (df_s.rdd.partitions.size > 1)
    assert (df_r.rdd.partitions.size == 1)

    val dfs = Array(df_t, df_s, df_r) 
    for (i <- 0 until dfs.size)
    {

        // TODO: We dont automatically test that anything is actually pushed down
        // but you can see them being pushed down by reading the memsql tracelog
      
        println(dfs(i).schema)
        println(dfs(i).rdd.toDebugString)
        var results = dfs(i).collect()
        println(results.size)
        println(dfs(i).rdd.partitions.size)
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
    
        assert(dfs(i).filter(dfs(i)("id").in(dfs(i)("id"))).collect().size == 1000)
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

object TestSaveToMemSQLVeryBasic {
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("MemSQLRDD Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val host = "127.0.0.1"
    val port = 10000
    val user = "root"
    val password = ""
    val dbName = "x_testsave"
    
    TestUtils.DropAndCreate(dbName)
    
    val rdd = sc.parallelize(
      Array(Row(1,"pieguy"),
            Row(2,"gbop"),
            Row(3,"berry\ndave"),
            Row(4,"psy\tduck"),
            Row(null,"null"),
            Row(6,"berry\\tdave"),
            Row(7,"berry\\ndave"),
            Row(8,"\"berry\" 'dave'")))

    val schema = StructType(Array(StructField("a",IntegerType,true),
                                  StructField("b",StringType,false)))
    val df1 = sqlContext.createDataFrame(rdd, schema)

    df1.createMemSQLTableAs(dbName, "t", host, port, user, password)

    val df_t = TestUtils.MemSQLDF(sqlContext,dbName, "t")
    assert(df_t.schema.equals(schema))
    assert(df_t.count == 8)
    assert(TestUtils.EqualDFs(df_t, df1))

    df1.saveToMemSQL(dbName, "t", host, port, user, password)

    assert(TestUtils.EqualDFs(df_t, df1.unionAll(df1)))
    
    // the column name matching should work
    df1.select("b","a").saveToMemSQL(dbName, "t", host, port, user, password)
    assert(TestUtils.EqualDFs(df_t, df1.unionAll(df1).unionAll(df1)))

    // and expressions and column renaming
    df1.where(df1("a") < 5).select(df1("a") + 1 as "b",df1("a")).saveToMemSQL(dbName, "t", host, port, user, password)    
    assert (df_t.filter(df_t("b") === "3").count == 1)

    

  }
}

object TestMemSQLTypes {
  def main(args: Array[String]) {
    val keyless = args.indexOf("keyless") != -1
    println("args.size = " + args.size)
    println("keyless = " + keyless)
    val conf = new SparkConf().setAppName("MemSQLRDD Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val host = "127.0.0.1"
    val port = 10000
    val user = "root"
    val password = ""
    val dbName = "alltypes_db"

    val dbAddress = "jdbc:mysql://" + host + ":" + port
    val conn = DriverManager.getConnection(dbAddress, user, password)
    val stmt = conn.createStatement
    stmt.executeQuery("set global plan_expiration_minutes = 0") // this way we can look in the plancache to see what queries were issued after the fact.  
    println("waiting for plans to flush")
    Thread sleep 60000
    stmt.executeQuery("set global plan_expiration_minutes = default") // this way we can look in the plancache to see what queries were issued after the fact.  

    TestUtils.DropAndCreate(dbName)
    
    val df_not_null = MemSQLTestSetup.SetupAllMemSQLTypes(sqlContext, false)
    val df_nullable = MemSQLTestSetup.SetupAllMemSQLTypes(sqlContext, true)
    
    assert(df_not_null.count == 3)
    assert(df_nullable.count == 4)
    assert(df_not_null.schema.size == Types.MemSQLTypes.size)
    assert(df_nullable.schema.size == Types.MemSQLTypes.size)

    for (i <- 0 until Types.MemSQLTypes.size)
    {
      val colname = Types.ToCol(Types.MemSQLTypes(i)._1)
      println(colname)

      assert(df_not_null.schema(i).dataType.equals(df_nullable.schema(i).dataType))
      assert(df_not_null.schema(i).name.equals(df_nullable.schema(i).name))
      assert(!df_not_null.schema(i).nullable)
      assert( df_nullable.schema(i).nullable)

      var cd_nn = df_not_null.select(colname).collect.map(_(0))
      var cd_na = df_nullable.select(colname).collect.map(_(0))
      println("not null")
      for (r <- cd_nn)
      {
        println(r)
      }
      println("nullable")
      for (r <- cd_na)
      {
        println(r)
      }
      println("reference")
      for (r <- Types.MemSQLTypes(i)._2)
      {
        println(r)
      }
      assert(cd_na.indexOf(null) != -1)
      assert(cd_nn.indexOf(null) == -1)
      
      assert(cd_na.filter(_ != null).map(_.toString).indexOf(Types.MemSQLTypes(i)._2(0)) != -1)
      assert(cd_na.filter(_ != null).map(_.toString).indexOf(Types.MemSQLTypes(i)._2(1)) != -1)
      assert(cd_na.filter(_ != null).map(_.toString).indexOf(Types.MemSQLTypes(i)._2(2)) != -1)

      assert(cd_nn.map(_.toString).indexOf(Types.MemSQLTypes(i)._2(0)) != -1)
      assert(cd_nn.map(_.toString).indexOf(Types.MemSQLTypes(i)._2(1)) != -1)
      assert(cd_nn.map(_.toString).indexOf(Types.MemSQLTypes(i)._2(2)) != -1)

    }

    val df_not_null2 = df_not_null.createMemSQLTableAs(dbName, "alltypes_not_null2", host, port, user, password, useKeylessShardedOptimization=keyless)
    val df_nullable2 = df_nullable.createMemSQLTableAs(dbName, "alltypes_nullable2", host, port, user, password, useKeylessShardedOptimization=keyless)

    println("df_not_null2")
    assert(TestUtils.EqualDFs(df_not_null, df_not_null2))
    println("df_nullable2")
    assert(TestUtils.EqualDFs(df_nullable, df_nullable2))
    
    // its too much to hope that the schema will be the same from an arbitrary table to one created with createMemSQLTableAs
    // but it shouldn't change on subsequent calls to createMemSQLTableAs
    //
    val df_not_null3 = df_not_null2.createMemSQLTableAs(dbName, "alltypes_not_null3", host, port, user, password, useKeylessShardedOptimization=keyless)
    val df_nullable3 = df_nullable2.createMemSQLTableAs(dbName, "alltypes_nullable3", host, port, user, password, useKeylessShardedOptimization=keyless)
    
    println(df_not_null3.schema)
    println(df_not_null2.schema)

    println(df_nullable3.schema)
    println(df_nullable2.schema)

    assert(df_not_null3.schema.equals(df_not_null2.schema))
    assert(df_nullable3.schema.equals(df_nullable2.schema))
    
    // If we are in keyless mode, the agg should have received no load data queries, since the loads should happen directly on the leaves.
    // Conversely, if we are not in keyless mode, the loads should happen on the agg.
    //
    val plans = MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.plancache where query_text like 'LOAD%'")).toArray
    assert(keyless == (plans.size == 0))
  }
}

object TestCreateWithKeys 
{
    def main(args: Array[String]) 
    {
        val conf = new SparkConf().setAppName("TestMemSQLContextVeryBasic")
        val sc = new MemSQLSparkContext(conf, "127.0.0.1", 10000, "root", "")
        val sqlContext = new SQLContext(sc)
        TestUtils.DropAndCreate("db")
      
        val rdd = sc.parallelize(Array[Row]())
        val schema = StructType(Array(StructField("a",IntegerType,true),
                                      StructField("b",StringType,false)))
        val df = sqlContext.createDataFrame(rdd, schema)

        df.createMemSQLTableFromSchema("db","t1", keys=Array(Shard()))
        df.createMemSQLTableFromSchema("db","t2", keys=Array(Shard("a")))
        df.createMemSQLTableFromSchema("db","t3", keys=Array(Shard("a","b")))
        df.createMemSQLTableFromSchema("db","t4", keys=Array(PrimaryKey("a","b"), Shard("a")))
        df.createMemSQLTableFromSchema("db","t5", keys=Array(UniqueKey("a","b"), Shard("a")))
        df.createMemSQLTableFromSchema("db","t6", keys=Array(PrimaryKey("a","b"), Key("b")))
        df.createMemSQLTableFromSchema("db","t7", keys=Array(Shard("a"), KeyUsingClusteredColumnStore("b")))

        val conn = sc.GetMAConnection
        var stmt = conn.createStatement
       
        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t1'")).toArray.size==0)

        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t2'")).toArray.size==1)
        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t2' and index_type='SHARD'")).toArray.size==1)

        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t3'")).toArray.size==2)
        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t3' and index_type='SHARD'")).toArray.size==2)

        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t4'")).toArray.size==3)
        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t4' and index_type='SHARD'")).toArray.size==1)
        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t4' and index_name='PRIMARY'")).toArray.size==2)

        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t5'")).toArray.size==3)
        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t5' and index_type='SHARD'")).toArray.size==1)
        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t5' and index_name='PRIMARY'")).toArray.size==0)

        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t6'")).toArray.size==5)
        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t6' and index_type='SHARD'")).toArray.size==2)
        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t6' and index_name='PRIMARY'")).toArray.size==4)

        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t7'")).toArray.size==2)
        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t7' and index_type='SHARD'")).toArray.size==1)
        assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t7' and index_type='CLUSTERED COLUMN'")).toArray.size==1)

    }
}
