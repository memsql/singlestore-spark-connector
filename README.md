# SingleStoreDB Spark Connector
## Version: 4.1.6 [![License](http://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

## Getting Started

You can find the latest version of the connector on Maven Central and
spark-packages.org. The group is `com.singlestore` and the artifact is
`singlestore-spark-connector_2.11` for Spark 2 and `singlestore-spark-connector_2.12` for Spark 3.

* [Maven Central (Spark 2)](https://search.maven.org/artifact/com.singlestore/singlestore-spark-connector_2.11)
* [Maven Central (Spark 3)](https://search.maven.org/artifact/com.singlestore/singlestore-spark-connector_2.12)
* [spark-packages.org](https://spark-packages.org/package/memsql/memsql-spark-connector)

You can add the connector to your Spark application using: spark-shell, pyspark, or spark-submit
```
$SPARK_HOME/bin/spark-shell --packages com.singlestore:singlestore-spark-connector_2.12:4.1.6-spark-3.5.0
```

We release multiple versions of the `singlestore-spark-connector`, one for each supported Spark version.
The connector follows the `x.x.x-spark-y.y.y` naming convention, where `x.x.x` represents the connector version 
and `y.y.y` represents the corresponding Spark version. 
For example, in connector `4.1.6-spark-3.5.0`, 4.1.6 is the version of the connector, 
compiled and tested against Spark version 3.5.0. 
It is critical to select the connector version that corresponds to the Spark version in use.

## Configuration

The `singlestore-spark-connector` is configurable globally via Spark options and
locally when constructing a DataFrame. The options are named the same, however
global options have the prefix `spark.datasource.singlestore.`.

#### Basic options
| Option                                              | Default value                                         | Description
| -                                                   | -                                                     | -
| `ddlEndpoint`    (On-Premise deployment) (required) | -                                                     | The hostname or IP address of the SingleStoreDB Master Aggregator in the `host[:port]` format, where port is an optional parameter. Example: `master-agg.foo.internal:3308` or `master-agg.foo.internal`.
| `dmlEndpoints`   (On-Premise deployment)            | ddlEndpoint                                           | The hostname or IP address of SingleStoreDB Aggregator nodes to run queries against in the `host[:port],host[:port],...` format, where :port is an optional parameter (multiple hosts separated by comma). Example: `child-agg:3308,child-agg2`.
| `clientEndpoint` (Cloud deployment) (required)      | -                                                     | The hostname or IP address to the SingleStoreDB Cloud workspace to run queries against in the format `host[:port]` (port is optional). Ex. `svc-b093ff56-7d9e-499f-b970-7913852facc4-ddl.aws-oregon-2.svc.singlestore.com:3306`
| `user`                                              | `root`                                                | The SingleStoreDB username.
| `password`                                          | -                                                     | Password of the SingleStoreDB user.
| `query`                                             | -                                                     | The query to run (mutually exclusive with dbtable).
| `dbtable`                                           | -                                                     | The table to query (mutually exclusive with query).
| `database`                                          | -                                                     | If set, all connections use the specified database by default.

#### Read options
| Option                                            | Default value                                         | Description
|---------------------------------------------------| -                                                     | -
| `disablePushdown`                                 | `false`                                               |Disable SQL Pushdown when running queries.
| `enableParallelRead`                              | `automaticLite`                                       | Enables reading data in parallel for some query shapes. It can have of the following values: `disabled`, `automaticLite`, `automatic`, and `forced`. For more information, see [Parallel Read Support](#parallel-read-support).
| `parallelRead.Features`                           | `ReadFromAggregators,ReadFromAggregatorsMaterialized` | Specifies a comma separated list of parallel read features that are tried in the order they are listed. SingleStore supports the following features: `ReadFromLeaves`, `ReadFromAggregators`, and `ReadFromAggregatorsMaterialized`. Example: `ReadFromAggregators,ReadFromAggregatorsMaterialized`. For more information, see [Parallel Read Support](#parallel-read-support).
| `parallelRead.tableCreationTimeoutMS`             | `0`                                                   | Specifies the amount of time (in milliseconds) the reader waits for the result table creation when using the `ReadFromAggregators` feature. If set to `0`, timeout is disabled.
| `parallelRead.materializedTableCreationTimeoutMS` | `0`                                                   | Specifies the amount of time (in milliseconds) the reader waits for the result table creation when using the `ReadFromAggregatorsMaterialized` feature. If set to `0`, timeout is disabled.
| `parallelRead.numPartitions`                      | `0`                                                   | Specifies the exact number of partitions in the resulting DataFrame. If set to `0`, value is ignored.
| `parallelRead.maxNumPartitions`                   | `0`                                                   | Specifies the Maximum number of partitions in the resulting DataFrame. If set to `0`, no limit is applied.
| `parallelRead.repartition`                        | `false`                                               | Repartition data before reading.
| `parallelRead.repartition.columns`                | `RAND()`                                              | Specifies a comma separated list of columns that are used for repartitioning (when `parallelRead.repartition` is enabled). By default, an additional column with `RAND()` value is used for repartitioning.

#### Write options
| Option                                             | Default value                                         | Description
| -                                                  | -                                                     | -
| `overwriteBehavior`                                | `dropAndCreate`                                       | Specifies the behavior during Overwrite. It can have one of the following values: `dropAndCreate`, `truncate`, `merge`.
| `truncate`                                         | `false`                                               | :warning: **This option is deprecated, please use `overwriteBehavior` instead.** Truncates instead of dropping an existing table during Overwrite.
| `loadDataCompression`                              | `Gzip`                                                | Compresses data on load. It can have one of the following three values: `GZip`, `LZ4`, and `Skip`.
| `loadDataFormat`                                   | `CSV`                                                 | Serializes data on load. It can have one of the following values: `Avro` or `CSV`.
| `tableKey`                                         | -                                                     | Specifies additional keys to add to tables created by the connector. See [Specifying keys for tables created by the Spark Connector](#specifying-keys-for-tables-created-by-the-spark-connector) for more information.
| `onDuplicateKeySQL`                                | -                                                     | If this option is specified and a new row with duplicate `PRIMARY KEY` or `UNIQUE` index is inserted, SingleStoreDB performs an `UPDATE` operation on the existing row. See [Inserting rows into the table with ON DUPLICATE KEY UPDATE](#inserting-rows-into-the-table-with-on-duplicate-key-update) for more information.
| `insertBatchSize`                                  | `10000`                                               | Specifies the size of the batch for row insertion.
| `maxErrors`                                        | `0`                                                   | The maximum number of errors in a single LOAD DATA request. When this limit is reached, the load fails. If this property is set to `0`, no error limit exists.
| `createRowstoreTable`                              | `rowstore`                                            | If enabled, the connector creates a rowstore table.

#### Connection pool options
| Option                                             | Default value                                         | Description
| -                                                  | -                                                     | -
| `driverConnectionPool.Enabled`                     | `true`                                                | Enables using of connection pool on the driver. (default: `true`)
| `driverConnectionPool.MaxOpenConns`                | `-1`                                                  | The maximum number of active connections with the same options that can be allocated from the driver pool at the same time, or negative for no limit. (default: `-1`)
| `driverConnectionPool.MaxIdleConns`                | `8`                                                   | The maximum number of connections with the same options that can remain idle in the driver pool, without extra ones being released, or negative for no limit. (default: `8`)
| `driverConnectionPool.MinEvictableIdleTimeMs`      | `30000` (30 sec)                                      | The minimum amount of time an object may sit idle in the driver pool before it is eligible for eviction by the idle object evictor (if any). (default: `30000` - 30 sec)
| `driverConnectionPool.TimeBetweenEvictionRunsMS`   | `1000` (1 sec)                                        | The number of milliseconds to sleep between runs of the idle object evictor thread on the driver. When non-positive, no idle object evictor thread will be run. (default: `1000` - 1 sec)
| `driverConnectionPool.MaxWaitMS`                   | `-1`                                                  | The maximum number of milliseconds that the driver pool will wait (when there are no available connections) for a connection to be returned before throwing an exception, or `-1` to wait indefinitely. (default: `-1`)
| `driverConnectionPool.MaxConnLifetimeMS`           | `-1`                                                  | The maximum lifetime in milliseconds of a connection. After this time is exceeded the connection will fail the next activation, passivation, or validation test and won’t be returned by the driver pool. A value of zero or less means the connection has an infinite lifetime. (default: `-1`)
| `executorConnectionPool.Enabled`                   | `true`                                                | Enables using of connection pool on executors. (default: `true`)
| `executorConnectionPool.MaxOpenConns`              | `true`                                                | The maximum number of active connections with the same options that can be allocated from the executor pool at the same time, or negative for no limit. (default: `true`)
| `executorConnectionPool.MaxIdleConns`              | `8`                                                   | The maximum number of connections with the same options that can remain idle in the executor pool, without extra ones being released, or negative for no limit. (default: `8`)
| `executorConnectionPool.MinEvictableIdleTimeMs`    | `2000`                                                | The minimum amount of time an object may sit idle in the executor pool before it is eligible for eviction by the idle object evictor (if any). (default: `2000` - 2 sec)
| `executorConnectionPool.TimeBetweenEvictionRunsMS` | `1000`                                                | The number of milliseconds to sleep between runs of the idle object evictor thread on the executor. When non-positive, no idle object evictor thread will be run. (default: `1000` - 1 sec)
| `executorConnectionPool.MaxWaitMS`                 | `-1`                                                  | The maximum number of milliseconds that the executor pool will wait (when there are no available connections) for a connection to be returned before throwing an exception, or `-1` to wait indefinitely. (default: `-1`)
| `executorConnectionPool.MaxConnLifetimeMS`         | `-1`                                                  | The maximum lifetime in milliseconds of a connection. After this time is exceeded the connection will fail the next activation, passivation, or validation test and won’t be returned by the executor pool. A value of zero or less means the connection has an infinite lifetime. (default: `-1`)

## Examples

### Configure `singlestore-spark-connector` for SingleStoreDB Cloud
The following example configures the `singlestore-spark-connector` globally:
```scala
spark.conf.set("spark.datasource.singlestore.clientEndpoint", "singlestore-host")
spark.conf.set("spark.datasource.singlestore.user", "admin")
spark.conf.set("spark.datasource.singlestore.password", "s3cur3-pa$$word")
```

The following example configures the `singlestore-spark-connector` using the read API:
```scala
val df = spark.read
    .format("singlestore")
    .option("clientEndpoint", "singlestore-host")
    .option("user", "admin")
    .load("foo")
```

The following example configures the `singlestore-spark-connector` using an external table in Spark SQL:
```sql
CREATE TABLE bar USING singlestore OPTIONS ('clientEndpoint'='singlestore-host','dbtable'='foo.bar')
```

> note: `singlestore-spark-connector`doesn't support writing to the reference table for SingleStoreDB Cloud
> note: `singlestore-spark-connector`doesn't support read-only databases for SingleStoreDB Cloud

### Configure `singlestore-spark-connector` for SingleStoreDB On-Premises
The following example configures the `singlestore-spark-connector` globally:
```scala
spark.conf.set("spark.datasource.singlestore.ddlEndpoint", "singlestore-master.cluster.internal")
spark.conf.set("spark.datasource.singlestore.dmlEndpoints", "singlestore-master.cluster.internal,singlestore-child-1.cluster.internal:3307")
spark.conf.set("spark.datasource.singlestore.user", "admin")
spark.conf.set("spark.datasource.singlestore.password", "s3cur3-pa$$word")
```

The following example configures the `singlestore-spark-connector` using the read API:
```scala
val df = spark.read
    .format("singlestore")
    .option("ddlEndpoint", "singlestore-master.cluster.internal")
    .option("user", "admin")
    .load("foo")
```

The following example configures the `singlestore-spark-connector` using an external table in Spark SQL:
```sql
CREATE TABLE bar USING singlestore OPTIONS ('ddlEndpoint'='singlestore-master.cluster.internal','dbtable'='foo.bar')
```

For Java/Python versions of some of these examples, visit the section ["Java & Python Example"](#java-python-example)

## Writing to SingleStoreDB

The `singlestore-spark-connector` supports saving dataframes to SingleStoreDB using the Spark write API. Here is a basic example of using this API:

```scala
df.write
    .format("singlestore")
    .option("loadDataCompression", "LZ4")
    .option("overwriteBehavior", "dropAndCreate")
    .mode(SaveMode.Overwrite)
    .save("foo.bar") // in format: database.table
```

If the target table ("foo" in the example above) does not exist in SingleStoreDB the
`singlestore-spark-connector` will automatically attempt to create the table. If you
specify SaveMode.Overwrite, if the target table already exists, it will be
recreated or truncated before load. Specify `overwriteBehavior = truncate` to truncate rather
than re-create.

### Retrieving the number of written rows from taskMetrics

It is possible to add the listener and get the number of written rows.

```scala
spark.sparkContext.addSparkListener(new SparkListener() {
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    println("Task id: " + taskEnd.taskInfo.id.toString)
    println("Records written: " + taskEnd.taskMetrics.outputMetrics.recordsWritten.toString)
  }
})

df.write.format("singlestore").save("example")
```

### Specifying keys for tables created by the Spark Connector
When creating a table, the `singlestore-spark-connector` will read options prefixed
with `tableKey`. These options must be formatted in a specific way in order to
correctly specify the keys.

 > :warning: The default table type is a SingleStoreDB columnstore.
 > To create a rowstore table instead, enable the `createRowstoreTable` option.

To explain we will refer to the following example:

```scala
df.write
    .format("singlestore")
    .option("tableKey.primary", "id")
    .option("tableKey.key.created_firstname", "created, firstName")
    .option("tableKey.unique", "username")
    .mode(SaveMode.Overwrite)
    .save("foo.bar") // in format: database.table
```

In this example, we are creating three keys:
1. A primary key on the `id` column
2. A regular key on the combination of the `firstname` and `created` columns, with the key name `created_firstname`
3. A unique key on the `username` column

Note on (2): Any key can optionally specify a name, just put it after the key type.
Key names must be unique.

To change the default ColumnStore sort key you can specify it explicitly:
```scala
df.write
    .option("tableKey.columnstore", "id")
```

You can also customize the shard key like so:
```scala
df.write
    .option("tableKey.shard", "id, timestamp")
```

## Inserting rows into the table with ON DUPLICATE KEY UPDATE

When updating a table it is possible to insert rows with `ON DUPLICATE KEY UPDATE` option.
See [sql reference](https://docs.singlestore.com/db/latest/en/reference/sql-reference/data-manipulation-language-dml/insert.html) for more details.
> :warning: This feature doesn't work for columnstore tables with SingleStoreDB 7.1.
```scala
df.write
    .option("onDuplicateKeySQL", "age = age + 1")
    .option("insertBatchSize", 300)
    .mode(SaveMode.Append)
    .save("foo.bar")
```

As a result of the following query, all new rows will be appended without changes.
If a row with the same `PRIMARY KEY` or `UNIQUE` index already exists then the corresponding `age` value will be increased.

When you use ON DUPLICATE KEY UPDATE, all rows of the DataFrame are split into batches, and every insert query will contain no more than the specified `insertBatchSize` rows setting.

## Save Modes

Save operations can optionally take a SaveMode, that specifies how to handle existing data if present.
It is important to realize that these save modes do not utilize any locking and are not atomic.

1. `SaveMode.Append` means that when saving a DataFrame to a data source, if data/table already exists,
contents of the DataFrame are expected to be appended to existing data.
2. `SaveMode.Overwrite` means that when saving a DataFrame to a data source,
if data/table already exists, existing data is expected to be overwritten by the contents of the DataFrame.
> `Overwrite` mode depends on `overwriteBehavior` option, for better understanding look at the section ["Merging on save"](#merging-on-save)
3. `SaveMode.ErrorIfExists` means that when saving a DataFrame to a data source,
if data already exists, an exception is expected to be thrown.
4. `SaveMode.Ignore` means that when saving a DataFrame to a data source, if data already exists, 
contents of the DataFrame are expected to be appended to existing data and all rows with duplicate key are ignored.

### Example of `SaveMode` option

```scala
df.write
    .mode(SaveMode.Append)
    .save("foo.bar")
```

<h2 id="merging-on-save">Merging on save</h2>

When saving dataframes or datasets to SingleStoreDB, you can manage how SaveMode.Overwrite is interpreted by the connector via the option overwriteBehavior.
This option can take one of the following values:

1. `dropAndCreate`(default) - drop and create the table before writing new values.
2. `truncate` - truncate the table before writing new values.
3. `merge` - replace rows with new rows by matching on the primary key.
(Use this option only if you need to fully rewrite existing rows with new ones.
To specify some rule for the update, use the `onDuplicateKeySQL` option instead.)

All these options are case-insensitive.

### Example of `merge` option

Suppose you have the following table, and the `Id` column is the primary key.

`SELECT * FROM <table>;`

| Id    | Name          | Age |
| ----- |:-------------:| ---:|
| 1     | Alice         | 20  |
| 2     | Bob           | 25  |
| 3     | Charlie       | 30  |

If you save the following dataframe with `overwriteBehavior = merge`:

| Id    | Name          | Age |
| ----- |:-------------:| ---:|
| 2     | Daniel        | 22  |
| 3     | Eve           | 27  |
| 4     | Franklin      | 35  |

```scala
df.write
    .format("singlestore")
    .option("overwriteBehavior", "merge")
    .mode(SaveMode.Overwrite)
    .save("<yourdb>.<table>")
```

After the save is complete, the table will look like this:
> note: rows with Id=2 and Id=3 were overwritten with new rows <br />
> note: the row with Id=1 was not touched and still exists in the result

`SELECT * FROM <table>;`

| Id    | Name          | Age |
| ----- |:-------------:| ---:|
| 1     | Alice         | 20  |
| 2     | Daniel        | 22  |
| 3     | Eve           | 27  |
| 4     | Franklin      | 35  |

## SQL Pushdown

The `singlestore-spark-connector` has extensive support for rewriting Spark SQL
and dataframe operation query plans into standalone SingleStoreDB queries. 
This allows most of the computation to be pushed into the SingleStoreDB distributed system 
without any manual intervention. The SQL rewrites are enabled automatically, 
but they can be disabled using the `disablePushdown` option.
The `singlestore-spark-connector` also support partial pushdown, 
where certain parts of a query can be evaluated in SingleStoreDB 
and certain parts can be evaluated in Spark.

> :warning: SQL Pushdown is either enabled or disabled on the *entire* Spark
> Session. If you want to run multiple queries in parallel with different
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

We also support most Spark SQL expressions. A full list of supported
operators/functions can be found in the
[ExpressionGen.scala](src/main/scala/com/singlestore/spark/ExpressionGen.scala) file.

The best place to look for examples of fully supported queries is in the tests.
Check out this file as a starting point:
[SQLPushdownTest.scala](src/test/scala/com/singlestore/spark/SQLPushdownTest.scala).

### Debugging SQL Pushdown

If you encounter an issue with SQL Pushdown the first step is to look at the
explain. You can do this easily from any dataframe using the function
`df.explain()`. If you pass the argument `true` you will get a lot more output
that includes pre and post optimization passes.

In addition, the `singlestore-spark-connector` outputs a lot of helpful information
when the TRACE log level is enabled for the `com.singlestore.spark` package.
To enable TRACE log level, add the following line(s) to the log4j configuration:

 - Log4j
```
log4j.logger.com.singlestore.spark=TRACE
```

 - Log4j 2
```
logger.singlestore.name = com.singlestore.spark
logger.singlestore.level = TRACE
logger.singlestore.additivity = false
```

Make sure not to leave it in place since it generates a huge amount of tracing
output.

## SQL Pushdown Incompatibilities
* `ToUnixTimestamp` and `UnixTimestamp` only handle time values less than `2038-01-19 03:14:08`, if they get `DateType` or `TimestampType` as a first argument.
* `FromUnixTime` with `yyyy-MM-dd HH:mm:ss` as the default format, only handles time less than `2147483648` (`2^31`).
* `DecimalType` is truncated on overflow (by default, Spark either throws an exception or returns null).
* `greatest` and `least` return `null` if at least one argument is `null` (in Spark these functions skip nulls).
* When a value can not be converted to numeric or fractional type SingleStoreDB returns 0 (Spark returns `null`).
* `Atanh(x)`, for x ∈ (-∞, -1] ∪ [1, ∞) returns, `null` (Spark returns `NaN`).
* When a string is cast to a numeric type, SingleStoreDB takes the prefix of it which is numeric (Spark returns `null` if the whole string is not numeric).
* When a numeric type is cast to a smaller one (in size), SingleStoreDB truncates it. For example `500` cast to the `Byte` will be `127`.
  Note: Spark optimizer can optimize casts for literals and then the behaviour for literals matches custom Spark behaviour.
* When a fractional type is cast to an integral type, SingleStoreDB rounds it to the closest value.
* `Log` returns `null` instead of `NaN`, `Infinity`, `-Infinity`.
* `Round` rounds down if the number to be rounded is followed by 5, and it is `DOUBLE` or `FLOAT` (`DECIMAL` is rounded up).
* `Conv` works differently if the number contains non-alphanumeric characters.
* `ShiftLeft`, `ShiftRight`, and `ShiftRightUnsigned` convert the value to an `UNSIGNED BIGINT` and then produce the shift.
  In case of an overflow, it returns `0` (`1<<64` = `0` and `10>>20` = `0`).
* `BitwiseGet` returns 0 when the bit position is negative or exceeds the bit upper limit.
* `Initcap` defines a letter as the beginning of a word even if it is enclosed in quotation marks, brackets, etc. For example "dear sir/madam (miss)" is converted to "Dear Sir/Madam (Miss)".
* `Skewness(x)`, in Spark 3.0, for `STD(x) = 0` returns `null` instead of `NaN`.

## Parallel Read Support
Parallel read can be enabled using `enableParallelRead` option. This can drastically improve performance in some cases.

The `enableParallelRead` option can have one of the following values:
 * `disabled`: Disables parallel reads and performs non-parallel reads.
 * `automaticLite`: Performs parallel reads if at least one parallel read feature specified in `parallelRead.Features` is supported. 
Otherwise performs a non-parallel read. In `automaticLite` mode, after push down of the outer sorting operation
(for example, a nested `SELECT` statement where sorting is done in a top-level `SELECT`) into SingleStoreDB is done, a non-parallel read is used.
 * `automatic`: Performs parallel reads if at least one parallel read feature specified in `parallelRead.Features` is supported. 
Otherwise performs a non-parallel read. In `automatic` mode, the `singlestore-spark-connector` is unable to push down an outer sorting operation into SingleStore. 
Final sorting is done at the Spark end of the operation.
 * `forced`: Performs parallel reads if at least one parallel read feature specified in `parallelRead.Features` is supported. 
Otherwise it returns an error. In `forced` mode, the `singlestore-spark-connector` is unable to push down an outer sorting operation into SingleStore. 
Final sorting is done at the Spark end of the operation.

By default, `enableParallelRead` is set to `automaticLite`.

### Parallel read features
The SingleStoreDB Spark Connector supports the following parallel read features:
 * `readFromAggregators`
 * `readFromAggregatorsMaterialized`
 * `readFromLeaves`

The connector uses the first feature specified in `parallelRead.Features` which meets all the requirements. 
The requirements for each feature are specified below. 
By default, the connector uses the `readFromAggregators` feature. 
You can repartition the result set for `readFromAggregators` and `readFromAggregatorsMaterialized` features. 
See [Parallel Read Repartitioning](#parallel-read-repartitioning) for more information.

#### readFromAggregators
When this feature is used, the `singlestore-spark-connector` will use [SingleStoreDB parallel read functionality](https://docs.singlestore.com/db/latest/en/query-data/query-procedures/read-query-results-in-parallel.html).
By default, the number of partitions in the resulting DataFrame is the least of the number of partitions in the SingleStoreDB database and Spark parallelism level
(i.e., sum of `(spark.executor.cores/spark.task.cpus)` for all executors).
Number of partitions in the resulting DataFrame can be controlled using `parallelRead.maxNumPartitions` and `parallelRead.numPartitions` options.
To use this feature, all reading queries must start at the same time. 
Connector tries to retrieve maximum number of tasks that can be run concurrently and uses this value to distribute reading queries.
In some cases, connector is not able to retrieve this value (for example, with AWS Glue). In such cases, `parallelRead.numPartitions` option is required.

Use the `parallelRead.tableCreationTimeoutMS` option to specify a timeout for result table creation.

Requirements:
 * SingleStoreDB version 7.5+
 * Either the `database` option is set, or the database name is specified in the `load` option
 * SingleStoreDB parallel read functionality supports the generated query
 * `parallelRead.numPartitioins` option is set, or connector is able to compute maximum number of concurrent tasks that can be run on Spark cluster

#### readFromAggregatorsMaterialized
This feature is very similar to `readFromAggregators`. The only difference is that `readFromAggregatorsMaterialized` uses the 
`MATERIALIZED` option to create the result table. When this feature is used, the reading tasks do not have to start at the same time. 
Hence, the parallelism level on the Spark cluster does not affect the reading tasks. 
Although, using the `MATERIALIZED` option may cause a query to fail if SingleStoreDB does not have enough memory to materialize the result set.
By default, the number of partitions in the resulting DataFrame is equal to the number of partitions in the SingleStoreDB database.
Number of partitions in the resulting DataFrame can be controlled using `parallelRead.maxNumPartitions` and `parallelRead.numPartitions` options.

Use the `parallelRead.materializedTableCreationTimeoutMS` option to specify a timeout for materialized result table creation.

Requirements:
 * SingleStoreDB version 7.5+
 * Either the `database` option is set, or the database name is specified in the `load` option
 * SingleStoreDB parallel read functionality supports the generated query

#### readFromLeaves
When this feature is used, the `singlestore-spark-connector` skips the transaction layer and reads directly from partitions on the leaf nodes.
Hence, each individual read task sees an independent version of the database's distributed state. 
If some queries (other than read operation) are run on the database, they may affect the current read operation.
Make sure to take this into account when using `readFromLeaves` feature.

This feature supports only those query-shapes that do not perform any operation on the aggregator and can be pushed down to the leaf nodes.

In order to use `readFromLeaves` feature, the username and password provided to the
`singlestore-spark-connector` must be the same across all nodes in the cluster.

By default, the number of partitions in the resulting DataFrame is equal to the number of partitions in the SingleStoreDB database.
Number of partitions in the resulting DataFrame can be controlled using `parallelRead.maxNumPartitions` and `parallelRead.numPartitions` options.

Requirements:
 * Either the `database` option is set, or the database name is specified in the `load` option
 * The username and password provided to the `singlestore-spark-connector` must be uniform across all the nodes in the cluster, 
because parallel reads require consistent authentication and connectible leaf nodes
 * The hostnames and ports listed by `SHOW LEAVES` must be directly connectible from Spark
 * The generated query can be pushed down to the leaf nodes

### Parallel read repartitioning
You can repartition the result using `parallelRead.repartition` option for the `readFromAggregators` and `readFromAggregatorsMaterialized` features 
to ensure that each task reads approximately the same amount of data.
This option is very useful for queries with top level limit clauses as without repartitioning it is possible that all rows will belong to one partition.

Use the `parallelRead.repartition.columns` option to specify a comma separated list of columns that will be used for repartitioning.
Column names that contain leading or trailing whitespaces or commas must be escaped as:
 - Column name must be enclosed in backticks
```
"a" -> "`a`"
```
 - Each backtick (`) in the column name must be replaced with two backticks (``)
```
"a`a``" -> "a``a````"
```

By default, repartitioning is done using an additional column with `RAND()` value.

### Example
```scala
spark.read.format("singlestore")
.option("enableParallelRead", "automatic")
.option("parallelRead.Features", "readFromAggregators,readFromLeaves")
.option("parallelRead.repartition", "true")
.option("parallelRead.repartition.columns", "a, b")
.option("parallelRead.TableCreationTimeout", "1000")
.load("db.table")
```

In the following example, connector will check requirements for `readFromAggregators`.
If they are satisfied, it will use this feature.
Otherwise, it will check requirements for `readFromLeaves`.
If they are satisfied, connector will use this feature. Otherwise, it will use non-parallel read.
If the connector uses `readFromAggregators`, it will repartition the result on the SingleStoreDB side before reading it,
and it will fail if creation of the result table will take longer than `1000` milliseconds.

## Running SQL queries
The methods `executeSinglestoreQuery(query: String, variables: Any*)` and `executeSinglestoreQueryDB(db: String, query: String, variables: Any*)`
allow you to run SQL queries on a SingleStoreDB database directly using the existing `SparkSession` object. The method `executeSinglestoreQuery`
uses the database defined in the `SparkContext` object you use. `executeSinglestoreQueryDB` allows you to specify the database that
will be used for querying.
The following examples demonstrate their usage (assuming you already have
initialized `SparkSession` object named `spark`). The methods return `Iterator[org.apache.spark.sql.Row]` object.

```scala
// this imports the implicit class QueryMethods which adds the methods
// executeSinglestoreQuery and executeSinglestoreQueryDB to SparkSession class
import com.singlestore.spark.SQLHelper.QueryMethods

// You can pass an empty database to executeSinglestoreQueryDB to connect to SingleStoreDB without specifying a database.
// This allows you to create a database which is defined in the SparkSession config for example.
spark.executeSinglestoreQueryDB("", "CREATE DATABASE foo")
// the next query can be used if the database field has been specified in spark object
s = spark.executeSinglestoreQuery("CREATE TABLE user(id INT, name VARCHAR(30), status BOOLEAN)")

// you can create another database
spark.executeSinglestoreQuery("CREATE DATABASE bar")
// the database specified as the first argument will override the database set in the SparkSession object
s = spark.executeSinglestoreQueryDB("bar", "CREATE TABLE user(id INT, name VARCHAR(30), status BOOLEAN)")
```

You can pass query parameters to the functions as arguments following `query`. The supported types for parameters are `String, Int, Long, Short, Float, Double, Boolean, Byte, java.sql.Date, java.sql.Timestamp`.
```scala
import com.singlestore.spark.SQLHelper.QueryMethods

var userRows = spark.executeSinglestoreQuery("SELECT id, name FROM USER WHERE id > ? AND status = ? AND name LIKE ?", 10, true, "%at%")

for (row <- userRows) {
  println(row.getInt(0), row.getString(1))
}
```
Alternatively, these functions can take `SparkSession` object as the first argument, as in the example below
```scala
import com.singlestore.spark.SQLHelper.{executeSinglestoreQuery, executeSinglestoreQueryDB}

executeSinglestoreQuery(spark, "CREATE DATABASE foo")
var s = executeSinglestoreQueryDB(spark, "foo", "SHOW TABLES")
```
## Security

### SQL Permissions

The [permission matrix](https://docs.singlestore.com/db/latest/en/reference/sql-reference/security-management-commands/permissions-matrix.html)
describes the permissions required to run each command.

To perform any SQL operation through the SingleStore Spark Connector, 
you must have the permissions required for that specific operation. 
The following matrix describes the minimum permissions required to perform some operations.
> Note: The ALL PRIVILEGES permission allows you to perform any operation.

| Operation                       | Min. Permission          | Alternative Permission |
| ------------------------------- |:------------------------:| ----------------------:|
| `READ` from collection          | `SELECT`                 | `ALL PRIVILEGES`       |
| `WRITE` to collection           | `SELECT, INSERT`         | `ALL PRIVILEGES`       |
| `DROP` database or collection   | `SELECT, INSERT, DROP`   | `ALL PRIVILEGES`       |
| `CREATE` database or collection | `SELECT, INSERT, CREATE` | `ALL PRIVILEGES`       |

For more information on granting privileges, see [GRANT](https://docs.singlestore.com/db/latest/en/reference/sql-reference/security-management-commands/grant.html)

### Connecting with a Kerberos-authenticated User

You can use the SingleStoreDB Spark Connector with a Kerberized user without any additional configuration.
To use a Kerberized user, you need to configure the connector with the given SingleStoreDB database user that is authenticated with Kerberos
(via the `user` option). Please visit our documentation [here](https://docs.singlestore.com/db/latest/en/security/authentication/kerberos-authentication.html)
to learn about how to configure SingleStoreDB users with Kerberos.

Here is an example of configuring the Spark connector globally with a Kerberized SingleStoreDB user called `krb_user`.

```scala
spark = SparkSession.builder()
    .config("spark.datasource.singlestore.user", "krb_user")
    .getOrCreate()
```

You do not need to provide a password when configuring a Spark Connector user that is Kerberized.
The connector driver (SingleStoreDB JDBC driver) will be able to authenticate the Kerberos user from the cache by the provided username.
Other than omitting a password with this configuration, using a Kerberized user with the Connector is no different than using a standard user.
Note that if you do provide a password, it will be ignored.

### SSL Support

The SingleStoreDB Spark Connector uses the SingleStoreDB JDBC Driver under the hood and thus
supports SSL configuration out of the box. In order to configure SSL, first
ensure that your SingleStoreDB cluster has SSL configured. Documentation on how to set
this up can be found here:
https://docs.singlestore.com/latest/guides/security/encryption/ssl/

Once you have setup SSL on your server, you can enable SSL via setting the following options:

```scala
spark.conf.set("spark.datasource.singlestore.useSSL", "true")
spark.conf.set("spark.datasource.singlestore.serverSslCert", "PATH/TO/CERT")
```

**Note:** the `serverSslCert` option may be server's certificate in DER form, or the server's
CA certificate. Can be used in one of 3 forms:

* `serverSslCert=/path/to/cert.pem` (full path to certificate)
* `serverSslCert=classpath:relative/cert.pem` (relative to current classpath)
* or as verbatim DER-encoded certificate string `------BEGIN CERTIFICATE-----...`

You may also want to set these additional options depending on your SSL configuration:

```scala
spark.conf.set("spark.datasource.singlestore.trustServerCertificate", "true")
spark.conf.set("spark.datasource.singlestore.disableSslHostnameVerification", "true")
```

See [The SingleStoreDB JDBC Driver](https://docs.singlestore.com/db/latest/en/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver.html#tls-parameters) 
for more information.

### JWT authentication

You may authenticate your connection to the SingleStoreDB cluster using the SingleStoreDB Spark connector with a JWT.
To use JWT-based authentication, specify the following parameters:
 - `credentialType=JWT` 
 - `password=<jwt-token>`

Here's a sample configuration that uses JWT-based authentication:
```
SparkConf conf = new SparkConf();
conf.set("spark.datasource.singlestore.ddlEndpoint", "singlestore-master.cluster.internal")
conf.set("spark.datasource.singlestore.dmlEndpoints", "singlestore-master.cluster.internal,singlestore-child-1.cluster.internal:3307")
conf.set("spark.datasource.singlestore.credentialType", "JWT")
conf.set("spark.datasource.singlestore.useSsl", "true")
conf.set("spark.datasource.singlestore.user", "s2user")
conf.set("spark.datasource.singlestore.password", "eyJhbGci.eyJzdWIiOiIxMjM0NTY3.masHf")
```

> note: To authenticate your connection to the SingleStoreDB cluster using the SingleStoreDB Spark connector with a JWT,
> the SingleStoreDB user must connect via SSL and use a JWT for authentication.
>
> See [Create a JWT User](https://docs.singlestore.com/managed-service/en/security/authentication/authenticate-via-jwt.html#create-a-jwt-user-751086) for more information.

See [Authenticate via JWT](https://docs.singlestore.com/managed-service/en/security/authentication/authenticate-via-jwt.html) for more information.

## Filing issues

When filing issues please include as much information as possible as well as any
reproduction steps. It's hard for us to reproduce issues if the problem depends
on specific data in your SingleStoreDB table for example. Whenever possible please try
to construct a minimal reproduction of the problem and include the table
definition and table contents in the issue.

If the issue is related to SQL Pushdown (or you aren't sure) make sure to
include the TRACE output (from the com.singlestore.spark package) or the full explain
of the plan. See the debugging SQL Pushdown section above for more information
on how to do this.

Happy querying!

## Setting up development environment

 * install Oracle JDK 8 from this url: https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html
 * install the community edition of Intellij IDEA from https://www.jetbrains.com/idea/
 * clone the repository https://github.com/memsql/singlestore-spark-connector.git
 * in Intellij IDEA choose `Configure->Plugins` and install Scala plugin
 * in Intellij IDEA run `Import Project` and select path to singlestore-spark-connector `build.sbt` file
 * choose `import project from external model` and `sbt`
 * in `Project JDK` select `New...->JDK` and choose the path to the installed JDK
 * `Finish`
 * it will overwrite some files and create build files (which are in .gitignore)
 * you may need to remove the `.idea` directory for IDEA to load the project properly
 * in Intellij IDEA choose `File->Close Project`
 * run `git checkout .` to revert all changes made by Intellij IDEA
 * in Intellij IDEA choose `Open` and select path to singlestore-spark-connector
 * run `Test Spark 3.0` (it should succeed)

<h2 id="java-python-example">Java & Python Examples</h2>

### Java

#### Configuration

```
SparkConf conf = new SparkConf();
conf.set("spark.datasource.singlestore.ddlEndpoint", "singlestore-master.cluster.internal")
conf.set("spark.datasource.singlestore.dmlEndpoints", "singlestore-master.cluster.internal,singlestore-child-1.cluster.internal:3307")
conf.set("spark.datasource.singlestore.user", "admin")
conf.set("spark.datasource.singlestore.password", "s3cur3-pa$$word")
```

#### Read Data

```
DataFrame df = spark
  .read()
  .format("singlestore")
  .option("ddlEndpoint", "singlestore-master.cluster.internal")
  .option("user", "admin")
  .load("foo");
```

#### Write Data

```
df.write()
    .format("singlestore")
    .option("loadDataCompression", "LZ4")
    .option("overwriteBehavior", "dropAndCreate")
    .mode(SaveMode.Overwrite)
    .save("foo.bar")
```

### Python

#### Configuration

```
spark.conf.set("spark.datasource.singlestore.ddlEndpoint", "singlestore-master.cluster.internal")
spark.conf.set("spark.datasource.singlestore.dmlEndpoints", "singlestore-master.cluster.internal,singlestore-child-1.cluster.internal:3307")
spark.conf.set("spark.datasource.singlestore.user", "admin")
spark.conf.set("spark.datasource.singlestore.password", "s3cur3-pa$$word")
```

#### Read Data

```
df = spark \
  .read \
  .format("singlestore") \
  .option("ddlEndpoint", "singlestore-master.cluster.internal") \
  .option("user", "admin") \
  .load("foo")
```

#### Write Data

```
df.write \
    .format("singlestore") \
    .option("loadDataCompression", "LZ4") \
    .option("overwriteBehavior", "dropAndCreate") \
    .mode("overwrite") \
    .save("foo.bar")
```
