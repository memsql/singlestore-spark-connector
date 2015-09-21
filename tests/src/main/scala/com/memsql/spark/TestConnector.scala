package com.memsql.spark

import com.memsql.spark.context.MemSQLContext
import java.sql.{DriverManager, ResultSet, Connection}

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.RowOrdering

import com.memsql.spark.connector._
import com.memsql.spark.connector.dataframe._
import com.memsql.spark.connector.rdd._

object MemSQLTestSetup {
  def SetupBasic() {
    val host = TestUtils.GetHostname
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
       CREATE /*!90618 reference */ TABLE r
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
    val host = TestUtils.GetHostname
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
    for (i <- 0 until 3) {
      insertQuery = insertQuery + "(" + Types.MemSQLTypes.map("'" + _._2(i) + "'").mkString(",") + ")"
      if (i < 2) {
        insertQuery = insertQuery + ","
      }
    }
    if (nullable) {
      insertQuery = insertQuery + ", (" + Types.MemSQLTypes.map((a:Any) => "null").mkString(",") + ")"
    }
    stmt.execute(insertQuery)
    return TestUtils.MemSQLDF(sqlContext, dbName, tbname)
  }

}
object TestUtils {
  def GetHostname: String = {
    import scala.sys.process._
    ("awk NR==1 /etc/hosts"!!).split("\t+")(0)
  }
  def DropAndCreate(dbName: String) {
    val host = TestUtils.GetHostname
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
      TestUtils.GetHostname,
      3306,
      "root",
      "",
      dbName,
      "SELECT * FROM " + tableName)
  }
  def CollectAndSort(df: DataFrame, asString: Boolean = false): Seq[Row] = {
    val rdd = if (asString) {
      df.rdd.map((r: Row) =>
        Row.fromSeq(r.toSeq.map(_.toString)))
    } else {
      df.rdd
    }

    rdd.collect.sorted(RowOrdering.forSchema(
      df.schema.map((sf:StructField) =>
        if (asString) { StringType } else sf.dataType)))
  }
  def EqualDFs(df1: DataFrame, df2: DataFrame, asString: Boolean = false): Boolean = {
    val df1_sorted = CollectAndSort(df1, asString)
    val df2_sorted = CollectAndSort(df2, asString)
    if (df1_sorted.size != df2_sorted.size) {
      println("len df1 = " + df1_sorted.size + ", len df2 = " + df2_sorted.size)
      return false
    }
    for (i <- 0 until df1_sorted.size) {
      if (!df1_sorted(i).equals(df2_sorted(i))) {
        println("row " + i + " is different.")
        if (df1_sorted(i).size != df2_sorted(i).size) {
          println("row sizes are different, " + df1_sorted(i).size + " vs " + df2_sorted(i).size)
          return false
        }
        for (r <- 0 until df1_sorted(i).size) {
          if ((df1_sorted(i)(r) == null) != (df2_sorted(i)(r) == null)
            || ((df1_sorted(i)(r) != null)  && !df1_sorted(i)(r).equals(df2_sorted(i)(r)))) {
            println("difference : " + df1_sorted(i)(r) + " vs " + df2_sorted(i)(r))
          }
        }
        return false
      }
    }
    true
  }

  def connectToMA: Connection = {
    val dbAddress = s"jdbc:mysql://${TestUtils.GetHostname}:3306"
    DriverManager.getConnection(dbAddress, "root", "")
  }
  def doDDL(conn: Connection, q: String) {
    val stmt = conn.createStatement
    stmt.execute(q)
    stmt.close()
  }
}
object Types {
  // We intentionally don't include memsql specific types (spatial+json),
  // and times that don't map to sparksql (time, unsigned)...
  val MemSQLTypes: Array[(String,Array[String])] = Array(
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
    ("date",Array("1990-08-23","1990-09-23","1990-10-23")))
  def ToCol(tp: String): String = "val_" + tp.replace("(","_").replace(")","").replace(",","_")
  val SparkSQLTypes: Array[(DataType,Array[Any])] = Array(
    (IntegerType,Array(1,2,3)),
    (LongType,Array(4,5,6)),
    (DoubleType,Array(7.8,9.1,1.2)),
    (FloatType,Array(2.8,3.1,4.2)),
    (ShortType,Array(7,8,9)),
    (ByteType,Array(10,11,12)),
    (BooleanType,Array(1,0,0)),
    (StringType,Array("hi","there","buddy")),
    (BinaryType,Array("how","are","you")),
    (TimestampType,Array("1990-08-23 01:01:04.0","1990-08-23 01:01:05.0","1990-08-23 01:01:06.0")),
    (DateType,Array("1990-08-23","1990-09-23","1990-10-23")))
}

object TestSparkSQLTypes {
  def main(args: Array[String]) = {
    val conn = TestUtils.connectToMA
    val conf = new SparkConf().setAppName("TestSparkSQLTypes")
    val sc = new SparkContext(conf)
    val sqlContext = new MemSQLContext(sc, TestUtils.GetHostname, 3306, "root", "")
    TestUtils.doDDL(conn, "DROP DATABASE IF EXISTS x_db")
    TestUtils.doDDL(conn, "CREATE DATABASE IF NOT EXISTS x_db")

    // Types.SparkSQLTypes (above) is an Array[(DataType,Array[Any])] where each element is a tuple (type, three possible values for that type)
    // We transpose this data into a dataframe, where each column has the name val_<typename>.
    //
    val schema = StructType(Types.SparkSQLTypes.map(r =>
      StructField("val_" + r._1.toString, r._1, true)))
    val rows = (0 until 3).map(i =>
      Row.fromSeq(Types.SparkSQLTypes.map(_._2(i)).toSeq))
    val df = sqlContext.createDataFrame(sc.parallelize(rows), schema)

    df.createMemSQLTableAs("x_db","t")
    val df2 = sqlContext.createDataFrameFromMemSQLTable("x_db","t")
    assert(TestUtils.EqualDFs(df, df2, asString=true))
  }
}

object TestMemSQLDataFrameVeryBasic {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MemSQLRDD Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val host = TestUtils.GetHostname
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
    for (i <- 0 until dfs.size) {
      // TODO: We dont automatically test that anything is actually pushed down
      // but you can see them being pushed down by reading the memsql tracelog

      println(dfs(i).schema)
      println(dfs(i).rdd.toDebugString)
      val results = dfs(i).collect()
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

    val host = TestUtils.GetHostname
    val port = 3306
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
      StructField("b",StringType,true)))
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
    Class.forName("com.mysql.jdbc.Driver")
    val keyless = args.indexOf("keyless") != -1
    println("args.size = " + args.size)
    println("keyless = " + keyless)
    val conf = new SparkConf().setAppName("MemSQLRDD Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val host = TestUtils.GetHostname
    val port = 3306
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

    for (i <- 0 until Types.MemSQLTypes.size) {
      val colname = Types.ToCol(Types.MemSQLTypes(i)._1)
      println(colname)

      assert(df_not_null.schema(i).dataType.equals(df_nullable.schema(i).dataType))
      assert(df_not_null.schema(i).name.equals(df_nullable.schema(i).name))
      assert( df_not_null.schema(i).nullable)
      assert( df_nullable.schema(i).nullable)

      var cd_nn = df_not_null.select(colname).collect.map(_(0))
      var cd_na = df_nullable.select(colname).collect.map(_(0))
      println("not null")
      for (r <- cd_nn) {
        println(r)
      }
      println("nullable")
      for (r <- cd_na) {
        println(r)
      }
      println("reference")
      for (r <- Types.MemSQLTypes(i)._2) {
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

object TestCreateWithKeys {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TestMemSQLContextVeryBasic")
    val sc = new SparkContext(conf)
    val sqlContext = new MemSQLContext(sc, TestUtils.GetHostname, 3306, "root", "")
    TestUtils.DropAndCreate("db")

    val rdd = sc.parallelize(Array[Row]())
    val schema = StructType(Array(StructField("a",IntegerType,true),
      StructField("b",StringType,false)))
    val df = sqlContext.createDataFrame(rdd, schema)

    df.createMemSQLTableFromSchema("db","t1", keys=List(Shard()))
    df.createMemSQLTableFromSchema("db","t2", keys=List(Shard("a")))
    df.createMemSQLTableFromSchema("db","t3", keys=List(Shard("a","b")))
    df.createMemSQLTableFromSchema("db","t4", keys=List(PrimaryKey("a","b"), Shard("a")))
    df.createMemSQLTableFromSchema("db","t5", keys=List(UniqueKey("a","b"), Shard("a")))
    df.createMemSQLTableFromSchema("db","t6", keys=List(PrimaryKey("a","b"), Key("b")))
    df.createMemSQLTableFromSchema("db","t7", keys=List(Shard("a"), KeyUsingClusteredColumnStore("b")))

    df.createMemSQLTableFromSchema("db","t8",
      extraCols=List(MemSQLExtraColumn("carl", "datetime", false)),
      keys=List(Shard(), KeyUsingClusteredColumnStore("carl"))
    )

    val conn = MemSQLContext.getMemSQLConnection(TestUtils.GetHostname, 3306, "root", "")
    val stmt = conn.createStatement

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

    assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t8'")).toArray.size==1)
    assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t8' and index_type='SHARD'")).toArray.size==0)
    assert(MemSQLRDD.resultSetToIterator(stmt.executeQuery("select * from information_schema.statistics where table_name='t8' and index_type='CLUSTERED COLUMN'")).toArray.size==1)
  }
}

object TestMemSQLContextVeryBasic {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TestMemSQLContextVeryBasic")
    val sc = new SparkContext(conf)
    val sqlContext = new MemSQLContext(sc, TestUtils.GetHostname, 3306, "root", "")
    TestUtils.DropAndCreate("db")

    assert(sqlContext.getMemSQLNodesAvailableForIngest.size == 2)
    assert(sqlContext.getMemSQLNodesAvailableForIngest(0).port == 3309)
    assert(sqlContext.getMemSQLNodesAvailableForIngest(1).port == 3310)

    assert(sqlContext.getMemSQLLeaves.size == 2)
    assert(sqlContext.getMemSQLLeaves(0).port == 3307)
    assert(sqlContext.getMemSQLLeaves(1).port == 3308)

    val rdd = sc.parallelize(
      Array(Row(1,"pieguy"),
        Row(2,"gbop"),
        Row(3,"berrydave"),
        Row(4,"psyduck"),
        Row(null,"null")),
      20)
    val schema = StructType(Array(StructField("a",IntegerType,true),
      StructField("b",StringType,false)))
    val df = sqlContext.createDataFrame(rdd, schema)

    val memdf =  df.createMemSQLTableAs("db","t")
    assert(TestUtils.EqualDFs(df, memdf))
    val memdf2 = df.createMemSQLTableAs("db","t2",TestUtils.GetHostname,3306,"root","")
    assert(TestUtils.EqualDFs(df, memdf2))

    // lets make sure colocation works.
    val targets = df.rdd.saveToMemSQLDryRun(sqlContext)
    assert (targets.exists(_.targetPort == 3309))
    assert (targets.exists(_.targetPort == 3310))
    for (t <- targets) {
      assert (t.isColocated)
      assert (t.targetPort == 3309 || t.targetPort == 3310)
    }
  }
}

object TestSaveToMemSQLErrors {
  def main(args: Array[String]) {
    val conn = TestUtils.connectToMA
    val conf = new SparkConf().setAppName("TestSaveToMemSQLErrors")
    val sc = new SparkContext(conf)
    val sqlContext = new MemSQLContext(sc, TestUtils.GetHostname, 3306, "root", "")
    TestUtils.doDDL(conn, "CREATE DATABASE IF NOT EXISTS x_db")

    val rdd = sc.parallelize(
      Array(Row(1,"pieguy"),
        Row(2,"gbop"),
        Row(3,"berry\ndave"),
        Row(4,"psy\tduck")))

    val schema = StructType(Array(StructField("a",IntegerType,true),
      StructField("b",StringType,false)))
    val df1 = sqlContext.createDataFrame(rdd, schema)

    try {
      df1.saveToMemSQL("x_db", "t")
      assert(false)
    } catch {
      case e: SaveToMemSQLException => {
        println(e.exception.getMessage)
        assert(e.exception.getMessage.contains("Table 'x_db.t' doesn't exist"))
      }
    }
    val df2 = df1.createMemSQLTableAs("x_db","t")
    for (dupKeySql <- Array("","b = 1")) {
      for (df <- Array(df1, df2)) {
        try {
          df.select(df("a") as "a", df("b") as "b", df("a") as "c").saveToMemSQL("x_db", "t", onDuplicateKeySql = dupKeySql)
        } catch {
          case e: SaveToMemSQLException => {
            assert(e.exception.getMessage.contains("Unknown column 'c' in 'field list'"))
          }
        }
        try {
          df.select(df("a"), df("b"), df("a")).saveToMemSQL("x_db", "t", onDuplicateKeySql = dupKeySql)
        } catch {
          case e: SaveToMemSQLException => {
            assert(e.exception.getMessage.contains("Column 'a' specified twice"))
          }
        }
      }
    }
  }
}

object TestSaveToMemSQLWithRDDErrors {
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("MemSQLRDD Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val host = TestUtils.GetHostname
    val port = 3306
    val user = "root"
    val password = ""
    val dbName = "x_testsave"

    TestUtils.DropAndCreate(dbName)

    val rdd = sc.parallelize(
      Array(Row(1,"pieguy")))
      .map(x => {
      throw new Exception("Test exception 123")
      x
    })

    val schema = StructType(Array(StructField("a",IntegerType,true),
      StructField("b",StringType,false)))
    val df1 = sqlContext.createDataFrame(rdd, schema)

    try {
      df1.createMemSQLTableAs(dbName, "t", host, port, user, password)
      assert(false, "We should have raised an exception when saving to MemSQL")
    } catch {
      case e: SaveToMemSQLException => {
        println(e.exception.getMessage)
        assert(e.exception.getMessage.contains("Test exception 123"))
      }
    }
  }
}

object TestSaveToMemSQLJSONColumn {
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("MemSQLRDD Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val host = TestUtils.GetHostname
    val port = 3306
    val user = "root"
    val password = ""
    val dbName = "x_testsave"

    TestUtils.DropAndCreate(dbName)

    val rdd = sc.parallelize(
      Array(Row(new JsonValue("{ \"foo\": \"bar\" }"))))

    val schema = StructType(Array(StructField("a", JsonType, true)))
    val df1 = sqlContext.createDataFrame(rdd, schema)

    df1.createMemSQLTableAs(dbName, "t", host, port, user, password)

    val df_t = TestUtils.MemSQLDF(sqlContext,dbName, "t")
    assert(df_t.count == 1)
    val result = df_t.select(df_t("a")).collect()(0).getAs[String]("a")
    println(result)
    assert(result == "{\"foo\":\"bar\"}")

    val dbAddress = "jdbc:mysql://" + host + ":" + port
    val conn = DriverManager.getConnection(dbAddress, user, password)
    val stmt = conn.createStatement
    val query = "select * from information_schema.columns where table_name='t' and column_name='a'"
    val columnTypes = MemSQLRDD.resultSetToIterator(stmt.executeQuery(query)).map((r: ResultSet) => r.getString("COLUMN_TYPE")).toArray
    val columnType = columnTypes(0)
    println(columnType)
    assert(columnType == "JSON")
  }
}

object TestLeakedConns {
  def main(args: Array[String]) {
    val conn = TestUtils.connectToMA
    TestUtils.doDDL(conn, "CREATE DATABASE IF NOT EXISTS x_db")
    println("sleeping for ten seconds while we let memsql set up the reference db")
    Thread.sleep(10000)
    val baseConns = numConns(conn)
    println ("base number connections = " + baseConns)
    val conf = new SparkConf().setAppName("TestSaveToMemLeakedConns")
    val sc = new SparkContext(conf)
    val sqlContext = new MemSQLContext(sc, TestUtils.GetHostname, 3306, "root", "")
    assert (baseConns == numConns(conn)) // creating the MemSQLContext shouldn't leak a connection

    TestUtils.doDDL(conn, "CREATE TABLE x_db.t(a bigint primary key, b bigint)")

    val rdd1 = sc.parallelize(Array(Row(1,1), Row(2,2), Row(3,3)))
    rdd1.saveToMemSQL("x_db",
                      "t",
                      sqlContext.getMemSQLMasterAggregator.host,
                      sqlContext.getMemSQLMasterAggregator.port,
                      sqlContext.getMemSQLUserName,
                      sqlContext.getMemSQLPassword)
    assert (baseConns == numConns(conn)) // successful saveToMemSQL shouldn't leak a connection
    rdd1.saveToMemSQL("x_db",
                      "t",
                      sqlContext.getMemSQLMasterAggregator.host,
                      sqlContext.getMemSQLMasterAggregator.port,
                      sqlContext.getMemSQLUserName,
                      sqlContext.getMemSQLPassword,
                      onDuplicateKeySql = "b = 1")
    assert (baseConns == numConns(conn)) // successful saveToMemSQL with upsert shouldn't leak a connection

    val rddnull = sc.parallelize(Array(Row(null,3)))
    for (dupKeySql <- Array("","b = 1")) {
      try {
        rddnull.saveToMemSQL("x_db",
                             "t",
                             sqlContext.getMemSQLMasterAggregator.host,
                             sqlContext.getMemSQLMasterAggregator.port,
                             sqlContext.getMemSQLUserName,
                             sqlContext.getMemSQLPassword,
                             onDuplicateKeySql = dupKeySql)
        assert (false)
      } catch {
        case e: SaveToMemSQLException => {
          assert(e.exception.getMessage.contains("NULL supplied to NOT NULL column 'a' at row 0")
            || e.exception.getMessage.contains("Column 'a' cannot be null"))
        }
      }
      assert (baseConns == numConns(conn)) // failed saveToMemSQL shouldn't leak a connection
    }

    val memrdd = sqlContext.createRDDFromMemSQLQuery("x_db","SELECT * FROM t")
    println(memrdd.collect()(0))
    assert (baseConns == numConns(conn)) // reading from MemSQLRDD shouldn't leak a connection

    val q = "SELECT a FROM t WHERE a < (SELECT a FROM t)" // query has runtime error because t has two rows
    val memrddfail = sqlContext.createRDDFromMemSQLQuery("x_db",q)
    try {
      println("before collect")
      println(memrddfail.collect()(0))
      println("after collect")
      assert(false)
    } catch {
      case e: SparkException => {
        println("in catch")
        assert(e.getMessage.contains("Subquery returns more than 1 row"))
      }
    }
    assert (baseConns == numConns(conn)) // failed reading from MemSQLRDD shouldn't leak a connection

    val df = sqlContext.createDataFrameFromMemSQLTable("x_db", "t")
    assert (baseConns == numConns(conn)) // getting metadata for dataframe shouldn't leak a connection

    df.createMemSQLTableFromSchema("x_db","s")
    assert (baseConns == numConns(conn)) // creating a table shouldn't leak a connection

    try {
      df.createMemSQLTableFromSchema("x_db","r",keys=List(PrimaryKey("a"), PrimaryKey("b")))
      assert(false)
    } catch {
      case e: Exception => {
        assert(e.getMessage.contains("Multiple primary key defined"))
      }
    }
    assert (baseConns == numConns(conn)) // failing to create a table shouldn't leak a connection
  }

  def numConns(conn: Connection) : Int = {
    val q = "SHOW STATUS LIKE 'THREADS_CONNECTED'"
    val stmt = conn.createStatement
    val result = MemSQLRDD.resultSetToIterator(stmt.executeQuery(q)).map((r:ResultSet) => r.getString("Value")).toArray
    println("num conns = " + result(0).toInt)
    for (r <- MemSQLRDD.resultSetToIterator(stmt.executeQuery("show processlist"))) {
      println("    processlist " + r.getString("Id") + " " + r.getString("db") + " " + r.getString("Command") + " "+ r.getString("State") + " " + r.getString("Info"))
    }
    stmt.close()
    result(0).toInt
  }
}

object TestEscapedColumnNames {
  def main(args: Array[String]) {
    val conn = TestUtils.connectToMA
    TestUtils.doDDL(conn, "CREATE DATABASE IF NOT EXISTS x_db")
    val conf = new SparkConf().setAppName("TestEscapedColumnNames")
    val sc = new SparkContext(conf)
    val sqlContext = new MemSQLContext(sc, TestUtils.GetHostname, 3306, "root", "")


    val schema = StructType(Array(StructField("index",IntegerType,true)))
    val rows = sc.parallelize(Array(Row(1),Row(2),Row(3),Row(4)))
    val df = sqlContext.createDataFrame(rows,schema)

    df.createMemSQLTableAs("x_db","t",
      keys=List(PrimaryKey("index")),
      extraCols=List(MemSQLExtraColumn("table", "varchar(200)", defaultValue="")))
  }
}
