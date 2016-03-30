MemSQL Spark Library
====================

This git repository contains a number of Scala projects that provide interoperation between [MemSQL](http://www.memsql.com) and a Spark cluster.

| Name | Description |
| ---- | ----------- |
| [MemSQL Spark Interface](#memsql-spark-interface) | A Spark app providing an API to run MemSQL Streamliner Pipelines on Spark |
| [MemSQL etlLib](#memsql-etllib) | A library of interfaces for building custom MemSQL Streamliner Pipelines |
| [MemSQL Spark Connector](#memsql-spark-connector) | Scala tools for connecting to MemSQL from Spark |

Supported Spark version
-----------------------

Right now this project is only supported for Spark version 1.5.2.  It has been
primarily tested against the MemSQL Spark Distribution which you can download
here: http://versions.memsql.com/memsql-spark/latest

Documentation
-------------

You can find Scala documentation for everything exposed in this repo here: [memsql.github.io/memsql-spark-connector](http://memsql.github.io/memsql-spark-connector)

You can find MemSQL documentation on our Spark ecosystem here: [docs.memsql.com/latest/spark/](http://docs.memsql.com/latest/spark/)

MemSQL Spark Interface
----------------------

The MemSQL Spark Interface is a Spark application that runs in a Spark cluster. The Interface provides an HTTP API to run real-time pipelines on Spark.  It is also required to interface [MemSQL Ops](http://docs.memsql.com/latest/ops/) with a Spark cluster.

MemSQL etlLib
-------------

The MemSQL ETL library provides interfaces and utilities required when writing custom pipeline JARs.  You can learn more about doing this [on our docs](http://docs_staging.memsql.com/latest/spark/memsql-spark-interface/).

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

The saveToMemsql function writes a `DataFrame` to a MemSQL table.

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

And our ETL interface for [MemSQL Streamliner](http://memsql.github.io/spark-streamliner):

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
