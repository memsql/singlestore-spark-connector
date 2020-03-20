# MemSQL Spark Connector
## Version: 3.0.0-beta2 [![Build Status](https://travis-ci.com/memsql/memsql-spark-connector.svg?branch=3.0.0-beta)](https://travis-ci.com/memsql/memsql-spark-connector) [![License](http://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

> :warning: **This is a beta release suitable for development and testing purposes.** 

## Major changes from the 2.0.0 connector

The MemSQL Spark Connector 3.0.0 has a number of key features and enhancements:

* Introduces SQL Optimization & Rewrite for most query shapes and compatible expressions
* Implemented as a native Spark SQL plugin
* Supports both the DataSource and DataSourceV2 API for maximum support of current and future functionality
* Contains deep integrations with the Catalyst query optimizer
* Is compatible with Spark 2.3
* Leverages MemSQL LOAD DATA to accelerate ingest from Spark via compression, vectorized cpu instructions, and optimized segment sizes
* Takes advantage of all the latest and greatest features in MemSQL 7.0

## Getting Started

You can find the latest version of the connector on Maven Central and
spark-packages.org.  The group is `com.memsql` and the artifact is
`memsql-spark-connector_2.11`.

* [Maven Central](https://search.maven.org/artifact/com.memsql/memsql-spark-connector_2.11)

We release two versions of the `memsql-spark-connector`, one per Spark version.
An example version number is: `3.0.0-beta-spark-2.3.4` which is the 3.0.0-beta
version of the connector, compiled and tested against Spark 2.3.4. Make sure
you are using the most recent version of the beta.

In addition to adding the `memsql-spark-connector`, you will also need to have the
MariaDB JDBC driver installed.  This library is tested against the following
MariaDB driver version:

```
"org.mariadb.jdbc" % "mariadb-java-client"  % "2.+"
```

Once you have everything installed, you're almost ready to run your first
queries against MemSQL!

## Configuration

The `memsql-spark-connector` is configurable globally via Spark options and
locally when constructing a DataFrame.  The options are named the same, however
global options have the prefix `spark.datasource.memsql.`.

| Option                    | Description
| -                         | -
| `ddlEndpoint`  (required) | Hostname or IP address of the MemSQL Master Aggregator in the format `host[:port]` (port is optional). Ex. `master-agg.foo.internal:3308` or `master-agg.foo.internal`
| `dmlEndpoints`            | Hostname or IP address of MemSQL Aggregator nodes to run queries against in the format `host[:port],host[:port],...` (port is optional, multiple hosts separated by comma). Ex. `child-agg:3308,child-agg2` (default: `ddlEndpoint`)
| `user`                    | MemSQL username (default: `root`)
| `password`                | MemSQL password (default: no password)
| `query`                   | The query to run (mutually exclusive with dbtable)
| `dbtable`                 | The table to query (mutually exclusive with query)
| `database`                | If set, all connections will default to using this database (default: empty)
| `disablePushdown`         | Disable SQL Pushdown when running queries (default: false)
| `enableParallelRead`      | Enable loading data in parallel for some query shapes (default: false)
| `truncate`                | Truncate instead of drop an existing table during Overwrite (default: false)
| `loadDataCompression`     | Compress data on load; one of (`GZip`, `LZ4`, `Skip`) (default: GZip)
| `tableKey`                | Specify additional keys to add to tables created by the connector (See below for more details)

Example of configuring the `memsql-spark-connector` globally:
```scala
spark.conf.set("spark.datasource.memsql.ddlEndpoint", "memsql-master.cluster.internal")
spark.conf.set("spark.datasource.memsql.dmlEndpoints", "memsql-master.cluster.internal,memsql-child-1.cluster.internal:3307")
spark.conf.set("spark.datasource.memsql.user", "admin")
spark.conf.set("spark.datasource.memsql.password", "s3cur3-pa$$word")
```

Example of configuring the `memsql-spark-connector` using the read API:
```scala
val df = spark.read
    .format("memsql")
    .option("ddlEndpoint", "memsql-master.cluster.internal")
    .option("user", "admin")
    .load("foo")
```

Example of configuring the `memsql-spark-connector` using an external table in Spark SQL:
```sql
CREATE TABLE bar USING memsql OPTIONS ('ddlEndpoint'='memsql-master.cluster.internal','dbtable'='foo.bar')
```

## Writing to MemSQL

The `memsql-spark-connector` supports saving dataframe's to MemSQL using the Spark write API. Here is a basic example of using this API:

```scala
df.write
    .format("memsql")
    .option("loadDataCompression", "LZ4")
    .option("truncate", "false")
    .mode(SaveMode.Overwrite)
    .save("foo.bar") // in format: database.table
```

If the target table ("foo" in the example above) does not exist in MemSQL the
`memsql-spark-connector` will automatically attempt to create the table. If you
specify SaveMode.Overwrite, if the target table already exists, it will be
recreated or truncated before load. Specify `truncate = true` to truncate rather
than re-create.

### Specifying keys for tables created by the Spark Connector
When creating a table, the `memsql-spark-connector` will read options prefixed
with `tableKey`. These options must be formatted in a specific way in order to
correctly specify the keys.

> :warning: The default table type is MemSQL Columnstore. If you want a RowStore table,
> you will need to specify a Primary Key using the tableKey option.

To explain we will refer to the following example:

```scala
df.write
    .format("memsql")
    .option("tableKey.primary", "id")
    .option("tableKey.key.created_firstname", "created, firstName")
    .option("tableKey.unique", "username")
    .mode(SaveMode.Overwrite)
    .save("foo.bar") // in format: database.table
```

In this example, we are creating three keys:
1. A primary key on the `id` column
2. A regular key on the columns `created, firstname` with the key name `created_firstname`
3. A unique key on the `username` column

Note on (2): Any key can optionally specify a name, just put it after the key type.
Key names must be unique.

To create a ColumnStore table you can specify a columnstore key:
```scala
df.write
    .option("tableKey.columnstore", "id")
```

You can also customize the shard key like so:
```scala
df.write
    .option("tableKey.shard", "id, timestamp")
```

## SQL Pushdown

The `memsql-spark-connector` has extensive support for rewriting Spark SQL query
plans into standalone MemSQL queries. This allows most of the computation to be
pushed into the MemSQL distributed system without any manual intervention. The
SQL rewrites are enabled automatically, but can be disabled either globally or
per-query using the `disablePushdown` option.

> :warning: SQL Pushdown is either enabled or disabled on the *entire* Spark
> Session.  If you want to run multiple queries in parallel with different
> values of `disablePushdown`, make sure to run them on separate Spark Sessions.

We currently support most of the primary Logical Plan nodes in Spark SQL
including:

 * Project
 * Filter
 * Aggregate
 * Window
 * Join
 * Limit
 * Sort

We also support most Spark SQL expressions.  A full list of supported
operators/functions can be found in the file
[ExpressionGen.scala](src/main/scala/com/memsql/spark/ExpressionGen.scala).

The best place to look for examples of fully supported queries is in the tests.
Check out this file as a starting point:
[SQLPushdownTest.scala](src/main/scala/com/memsql/spark/SQLPushdownTest.scala).

### Debugging SQL Pushdown

If you encounter an issue with SQL Pushdown the first step is to look at the
explain.  You can do this easily from any dataframe using the function
`df.explain()`.  If you pass the argument `true` you will get a lot more output
that includes pre and post optimization passes.

In addition, the `memsql-spark-connector` outputs a lot of helpful information
when the TRACE log level is enabled for the `com.memsql.spark` package.  You can
do this in your log4j configuration by adding the following line:

```
log4j.logger.com.memsql.spark=TRACE
```

Make sure not to leave it in place since it generates a huge amount of tracing
output.

## Filing issues

When filing issues please include as much information as possible as well as any
reproduction steps. It's hard for us to reproduce issues if the problem depends
on specific data in your MemSQL table for example.  Whenever possible please try
to construct a minimal reproduction of the problem and include the table
definition and table contents in the issue.

If the issue is related to SQL Pushdown (or you aren't sure) make sure to
include the TRACE output (from the com.memsql.spark package) or the full explain
of the plan.  See the debugging SQL Pushdown section above for more information
on how to do this.

Happy querying!
