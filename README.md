MemSQL Spark Connector
======================

The MemSQL Spark connector provides tools for reading from and writing to
MemSQL databases in Spark.

There are two parts to the connector; the MemSQLRDD and the saveToMemsql
function.  The former allows users to read data out of a MemSQL database; the
latter allows users to write to a MemSQL database.

MemSQLRDD
---------

The MemSQLRDD reads rows out of a MemSQL database.

```
import com.memsql.spark.connector.rdd.MemSQLRDD

...

val rdd = new MemSQLRDD(
    sc,
    dbHost,
    dbPort,
    dbUser,
    dbPassword,
    dbName,
    "SELECT * FROM test_table",
    (r: ResultSet) => { r.getString("test_column") })
rdd.first()  // Contains the value of "test_column" for the first row
```

Note that you can provide a mapRow function that can map rows in the query
results to a return type of your choice.

saveToMemsql
------------

The saveToMemsql function writes an array-based RDD to a MemSQL table.

```
import com.memsql.spark.connector._

...

val rdd = sc.parallelize(Array(Array("foo", "bar"), Array("baz", "qux")))
rdd.saveToMemsql(dbHost, dbPort, dbUser, dbPassword, dbName, outputTableName, insertBatchSize=1000)
```

Building
--------
Run ``make package`` to compile this connector.  This will create a
directory called ``distribution/dist/memsqlrdd-<version number>``, which will
contain a .jar file.  Simply put this .jar file in your class path to
use this connector.

This directory will also contain HTML documentation for the connector.

In order to compile this connector, you must have the [Simple Build
Tool (aka sbt)](http://www.scala-sbt.org/) installed.

Tweaks
------
The saveToMemsql function has an optional insertBatchSize argument. This
controls the size of the INSERT queries that we generate when inserting data
into MemSQL.  We build these queries in-memory, so if you find that your
executors are running out of memory, consider lowering this value.
