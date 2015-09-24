MemSQL Spark Library
====================

This git repository contains a number of Scala projects that provide interoperation between [MemSQL](http://www.memsql.com) and a Spark cluster.

| Name | Description |
| ---- | ----------- |
| [MemSQL Spark Interface](#memsql-spark-interface) | A Spark app providing an API to run MemSQL Streamliner Pipelines on Spark |
| [MemSQL etlLib](#memsql-etllib) | A library of interfaces for building custom MemSQL Streamliner Pipelines |
| [MemSQL Spark Connector](#memsql-spark-connector) | Scala tools for connecting to MemSQL from Spark |

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

The connector provides a number of intergrations with Apache Spark including a custom RDD type, dataframe helpers and a MemSQL Context.

###MemSQLContext

The MemSQL Context maintains metadata about a MemSQL cluster and extends the Spark SQLContext.

```
import com.memsql.spark.connector.context.MemSQLContext

// note: The connection details below should point at your MemSQL Master Aggregator
val memsqlContext = new MemSQLContext(sparkContext, dbHost, dbPort, dbUser, dbPass)

val myTableDF = memsqlContext.createDataFrameFromMemSQLTable("my_database", "my_table")
// myTableDF now is a Spark DataFrame which represents the specified MemSQL table
// and can be queried using Spark DataFrame query functions
```

###MemSQL Data Frame Support

You can use the constructor `MemSQLDataFrame.MakeMemSQLDF` to construct a Spark DataFrame from a MemSQL table.

```
import com.memsql.spark.connector.dataframe.MemSQLDataFrame

val df = MemSQLDataFrame.MakeMemSQLDF(
    sqlContext, dbHost, dbPort,
    dbUser, dbPassword, dbName,
    "SELECT * FROM test_table")

val result = df.select(df("test_column")).where(df("other_column") === 1).limit(1)
// Result now contains the first row where other_column == 1
```

###MemSQLRDD

The MemSQLRDD reads rows out of a MemSQL database.

```
import com.memsql.spark.connector.rdd.MemSQLRDD

...

val rdd = new MemSQLRDD(
    sparkContext, dbHost, dbPort,
    dbUser, dbPassword, dbName,
    "SELECT * FROM test_table",
    (r: ResultSet) => { r.getString("test_column") })

rdd.first()  // Contains the value of "test_column" for the first row
```

Note that you can provide a mapRow function that can map rows in the query
results to a return type of your choice.

###saveToMemsql

The saveToMemsql function writes an array-based RDD to a MemSQL table.

```
import com.memsql.spark.connector._

...

val rdd = sc.parallelize(Array(Array("foo", "bar"), Array("baz", "qux")))
rdd.saveToMemsql(dbHost, dbPort, dbUser, dbPassword, dbName, outputTableName, insertBatchSize=1000)
```

Using
-----

The recommended way to depend on the above projects is via our private Maven repository [maven.memsql.com](maven.memsql.com).  You can do this in your build.sbt file for [Simple Build
Tool (aka sbt)](http://www.scala-sbt.org/) integration.  Add the following line at the top level of your build.sbt to add our repo:

```
resolvers += "memsql" at "http://maven.memsql.com"
```

Then inside a project definition you can depend on our libraries like so:

```
libraryDependencies  += "com.memsql" %% "memsqletl" % "VERSION"
```

We also support packaging the MemSQL Spark Connector from source. Run ``make package`` to compile this connector.  This will create a directory called ``distribution/dist/memsql-<version number>``, which will contain a .jar file. Simply put this .jar file in your class path to use this library.

In order to compile this library you must have the [Simple Build
Tool (aka sbt)](http://www.scala-sbt.org/) installed.

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
sbt 'project interface' test
```

Tweaks
------
The saveToMemsql function has an optional insertBatchSize argument. This
controls the size of the INSERT queries that we generate when inserting data
into MemSQL.  We build these queries in-memory, so if you find that your
executors are running out of memory, consider lowering this value.
