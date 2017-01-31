MemSQL Spark 2.0 Connector
====================

This git repository contains the MemSQL 2.0 Spark connector, which enables users to load data from MemSQL tables into Spark Dataframes, and write Spark Dataframes to MemSQL tables.


Supported Spark version
-----------------------

Right now this project is only supported for Spark version 2.0.0+.  It has been
primarily tested against the Spark version 2.0.2
Documentation
-------------

You can find Scala documentation for everything exposed in this repo here: [memsql.github.io/memsql-spark-connector](http://memsql.github.io/memsql-spark-connector)

You can find MemSQL documentation on our Spark ecosystem here: [docs.memsql.com/latest/spark/](http://docs.memsql.com/latest/spark/)


MemSQL Spark Connector
----------------------

The MemSQL Spark connector provides tools for reading from and writing to
MemSQL databases in Spark.

The connector provides a number of integrations with Apache Spark including a custom RDD type, DataFrame helpers and a MemSQL Context.

###MemSQLContext

The MemSQL Context maintains metadata about a MemSQL cluster and extends the Spark SQLContext.

```
import com.memsql.spark.connector.MemSQLContext

// NOTE: The connection details for your MemSQL Master Aggregator must be in
// the Spark configuration. See http://memsql.github.io/memsql-spark-connector/latest/api/#com.memsql.spark.connector.MemSQLConf
// for details.
val memsqlContext = new MemSQLContext(sparkContext)

val myTableDF = memsqlContext.table("my_table")
// myTableDF now is a Spark DataFrame which represents the specified MemSQL table
// and can be queried using Spark DataFrame query functions
```

You can also use `memsqlContext.sql` to pull arbitrary tables and expressions
into a `DataFrame`

```
val df = memsqlContext.sql("SELECT * FROM test_table")

val result = df.select(df("test_column")).where(df("other_column") === 1).limit(1)
// Result now contains the first row where other_column == 1
```

Additionally you can use the `DataFrameReader` API
```
val df = memsqlContext.read.format("com.memsql.spark.connector").load("db.table")
```

###saveToMemsql

// count the number of rows
println(s"The number of customers from Illinois is ${customersFromIllinois.count()}")

```
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.memsql.SparkImplicits._

...

val rdd = sc.parallelize(Array(Row("foo", "bar"), Row("baz", "qux")))
val schema = StructType(Seq(StructField("col1", StringType, false),
                            StructField("col2", StrindType, false)))
val df = sqlContext.createDataFrame(rdd, schema)
df.saveToMemSQL("db", "table")
```

You can also use the `DataFrameWriter` API
```
df.write.format("com.memsql.spark.connector").save("db.table")
```

Using
-----

In order to compile this library you must have the [Simple Build
Tool (aka sbt)](http://www.scala-sbt.org/) installed.

Artifacts are published to Maven Central [http://repo1.maven.org/maven2/com/memsql/].

Inside a project definition you can depend on our MemSQL Connector like so:

```
libraryDependencies  += "com.memsql" %% "memsql-connector" % "VERSION"
```

The `mode` specifies how to handle existing data if present. The default, if unspecified, is "error", which means that if data already exists in people.students, an error is to be thrown.

```
libraryDependencies  += "com.memsql" %% "memsql-etl" % "VERSION"
```

Building
--------

You can use SBT to compile all of the projects in this repo.  To build all of the projects you can use:

```
sbt "project etlLib" build \
    "project connectorLib" build \
    "project interface" build
```

Testing
-------

All unit tests can be run via sbt.  They will also run at build time automatically.

```
sbt 'project etlLib' test
sbt 'project connectorLib' test
sbt 'project interface' test
```
