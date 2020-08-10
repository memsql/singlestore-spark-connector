MemSQL Spark Connector 2.0
====================
The MemSQL Spark connector 2.0 enables users to load data from MemSQL tables into Spark Dataframes, and write Spark Dataframes to MemSQL tables.

:warning: **MemSQL Spark Connector Version 2.0 has a formal deprecation date of October 1st, 2020. Please upgrade to the latest production version [MemSQL Spark Connector 3.0](https://github.com/memsql/memsql-spark-connector)**


Requirements
------------

This library requires Spark 2.0+ and has been primarily tested against Spark version 2.0.2. For support with Spark 1.x, please check the [1.x branch](https://github.com/memsql/memsql-spark-connector/tree/1.3.3).


Documentation
-------------

You can find Scala documentation for everything exposed in this repo here: [memsql.github.io/memsql-spark-connector](http://memsql.github.io/memsql-spark-connector)


Installation
------------

Inside a project definition you can depend on the MemSQL Connector using sbt:

```
libraryDependencies  += "com.memsql" %% "memsql-connector" % "2.0.7"
```

or Maven:

```xml
<dependency>
    <groupId>com.memsql</groupId>
    <artifactId>memsql-connector_2.11</artifactId>
    <version>2.0.7</version>
</dependency>
```


Usage
-----

MemSQL Spark Connector leverages Spark SQL's Data Sources API. The connection to MemSQL relies on the following Spark configuration settings.

| Setting name                             | Default value if not specified  |
| --------------------                     | ------------------------------  |
| spark.memsql.host                        | localhost                       |
| spark.memsql.port                        | 3306                            |
| spark.memsql.user                        | root                            |
| spark.memsql.password                    | None                            |
| spark.memsql.defaultDatabase             | None                            |
| spark.memsql.defaultSaveMode             | "error" (see description below) |
| spark.memsql.disablePartitionPushdown    | false                           |
| spark.memsql.defaultCreateMode           | DatabaseAndTable                |

`defaultCreateMode` specifies whether the connector will create the database and/or table if it doesn't already exist, when saving data to MemSQL. The possible values are `DatabaseAndTable`, `Table`, and `Skip`. The user will need the corresponding create permissions if the value is not `Skip`.

Note that all MemSQL credentials have to be the same on all nodes to take advantage of partition pushdown, which queries leaves directly.

### Loading data from MemSQL

The following example creates a Dataframe from the table "illinois" in the database "customers". To use the library, pass in "com.memsql.spark.connector" as the `format` parameter so Spark will call the MemSQL Spark Connector code. The option `path` is the full path of the table using the syntax `$database_name`.`$table_name`. If there is only a table name, the connector will look for the table in the default database set in the configuration.

```scala
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

val conf = new SparkConf()
	.setAppName("MemSQL Spark Connector Example")
	.set("spark.memsql.host", "10.0.0.190")
	.set("spark.memsql.password", "foobar")
	.set("spark.memsql.defaultDatabase", "customers")
val spark = SparkSession.builder().config(conf).getOrCreate()

val customersFromIllinois = spark
	.read
	.format("com.memsql.spark.connector")
	.options(Map("path" -> ("customers.illinois")))
	.load()
// customersFromIllinois is now a Spark DataFrame which represents the specified MemSQL table
// and can be queried using Spark DataFrame query functions

// count the number of rows
println(s"The number of customers from Illinois is ${customersFromIllinois.count()}")

// print out the DataFrame
customersFromIllinois.show()
```

Instead of specifying a MemSQL table as the `path` in the options, the user can opt to create a DataFrame from a SQL query with the option `query`. This can minimize the amount of data transferred from MemSQL to Spark, and push down distributed computations to MemSQL instead of Spark. For best performance, either specify the database name using the option `database`, OR make sure a default database is set in the Spark configuration. Either setting enables the connector to query the MemSQL leaf nodes directly, instead of going through the master aggregator.

```scala
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

val conf = new SparkConf()
	.setAppName("MemSQL Spark Connector Example")
	.set("spark.memsql.host", "10.0.0.190")
	.set("spark.memsql.password", "foobar")
val spark = SparkSession.builder().config(conf).getOrCreate()

val customersFromIllinois = spark
	.read
	.format("com.memsql.spark.connector")
	.options(Map("query" -> ("select age_group, count(*) from customers.illinois where number_of_orders > 3 GROUP BY age_group"),
				 "database" -> "customers"))
	.load()

customersFromIllinois.show()
// +-----------+---------+
// | age_group | count(*)|
// +-----------+---------+
// |  13-18    |   128   |
// |  19-25    |   150   |
// |  26+      |   140   |
// +-----------+---------+
```

### Saving data to MemSQL

Similarly, use Spark SQL's Data Sources API to save a DataFrame to MemSQL. To save a DataFrame in the MemSQL table "students":

```scala
...

val rdd = sc.parallelize(Array(Row("John Smith", 12), Row("Jane Doe", 13)))
val schema = StructType(Seq(StructField("Name", StringType, false),
                            StructField("Age", IntegerType, false)))
val df = sqlContext.createDataFrame(rdd, schema)
df
	.write
	.format("com.memsql.spark.connector")
	.mode("error")
	.save("people.students")
```

The `mode` specifies how to handle duplicate keys when the MemSQL table has a primary key. The default, if unspecified, is "error", which means that if a row with the same primary key already exists in MemSQL's people.students table, an error is to be thrown. Other save modes:

| Save mode string | Description                                                                                                            |
| -----------------| -----------                                                                                                            |
| "error"          | MemSQL will error when encountering a record with duplicate keys                                                       |
| "ignore"         | MemSQL will ignore records with duplicate keys and, without rolling back, continue inserting records with unique keys. |
| "overwrite"      | MemSQL will replace the existing record with the new record                                                            |

Other MemSQL write settings can be specified using `.option(...)` or `.options(...)`.  To perform a dry run of the previous example:

```scala
df
	.write
	.format("com.memsql.spark.connector")
	.mode("error")
	.option("dryRun", "true")
	.save("people.students")
```

| Option name         | Description                                                                                          |
| ------------------- | ---------------------------------------------------------------------------------------------------- |
| writeToMaster       | Force this write to be sent to the master aggregator                                                 |
| dryRun              | Don't actually perform the write (this will still create the database and table if they don't exist) |
| saveMode            | See Spark configuration settings                                                                     |
| createMode          | See Spark configuration settings                                                                     |
| insertBatchSize     | See Spark configuration settings                                                                     |
| loadDataCompression | See Spark configuration settings                                                                     |


The second interface to save data to MemSQL is via the `saveToMemSQL` implicit function on a DataFrame you wish to save:
```scala
...

val rdd = sc.parallelize(Array(Row("John Smith", 12), Row("Jane Doe", 13)))
val schema = StructType(Seq(StructField("Name", StringType, false),
                            StructField("Age", IntegerType, false)))
val df = sqlContext.createDataFrame(rdd, schema)
df.saveToMemSQL("people.students")
      // The database name can be omitted if "spark.memsql.defaultDatabase" is set
      // in the Spark configuration df.sqlContext.sparkContext.getConf.getAll
```

A call to `saveToMemSQL` can take three forms:
```scala
# Table only
df.saveToMemSQL("tbl")

# Database and table
df.saveToMemSQL("db", "tbl")

# Database, table, and options
val saveConf = SaveToMemSQLConf(ss.memSQLConf, params=Map("dryRun" -> "true"))
df.saveToMemSQL(TableIdentifier("db", "tbl"), saveConf)
```

Any options not specified in `saveConf` will default to those in the `MemSQLConf`.


Types
-----

When saving a Dataframe from Spark to MemSQL, the SparkType of each Dataframe column will be converted to the following MemSQL type:

| SparkType     | MemSQL Type |
| ---------     | ----------- |
| ShortType     | SMALLINT    |
| FloatType     | FLOAT       |
| DoubleType    | DOUBLE      |
| LongType      | BIGINT      |
| IntegerType   | INT         |
| BooleanType   | BOOLEAN     |
| StringType    | TEXT        |
| BinaryType    | BLOB        |
| DecimalType   | DECIMAL     |
| TimeStampType | TIMESTAMP   |
| DateType      | DATE        |

When reading a MemSQL table as a Spark Dataframe, the MemSQL column type will be converted to the following SparkType:

| MemSQL Type       | SparkType     |
| -----------       | ----------    |
| TINYINT, SMALLINT | ShortType     |
| INTEGER           | IntegerType   |
| BIGINT (signed)   | LongType      |
| DOUBLE, NUMERIC   | DoubleType    |
| REAL              | FloatType     |
| DECIMAL           | DecimalType   |
| TIMESTAMP         | TimestampType |
| DATE              | DateType      |
| TIME              | StringType    |
| CHAR, VARCHAR     | StringType    |
| BIT, BLOG, BINARY | BinaryType    |

MemSQL Spark 2.0 Connector does not support GeoSpatial or JSON MemSQL types since Spark 2.0 has currently disabled user defined types (see [JIRA issue](https://issues.apache.org/jira/browse/SPARK-14155)). These types, when read, will become BinaryType.

Changes from MemSQL Spark 1.X Connector
---------------------------------------

While the MemSQL Spark 1.X Connector relied on Spark SQL experimental developer APIs, the MemSQL Spark 2.0 Connector uses only the official and stable APIs for loading data from an external data source documented [here](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.sources.package). In certain cases, we can "push down" distributed computations to MemSQL. This means that instead of having Spark perform a a transformation (eg. filter, join, etc) on the data it retrieved from MemSQL, you can let MemSQL do the operation on the data and pass the result to Spark. The MemSQL Spark 2.0 Connector supports column and filter pushdown; if you would like to push down joins or aggregates, consider explicitly including it in the user-specified `query` option. E.g. instead of

```scala
val people = spark.read.format("com.memsql.spark.connector").options(Map("path" -> ("db.people"))).load()
val department = spark.read.format("com.memsql.spark.connector").options(Map("path" -> ("db.department"))).load()
val result = people.join(department, people("deptId") === department("id"))
```
Do:

```scala
val result = spark
	.read
	.format("com.memsql.spark.connector")
	.options(Map("query" -> ("select * from people join department on people.deptId = department.id")))
	.load()
```

Building and Testing
--------------------

You can use SBT to compile the library

```
sbt compile
```

All unit tests can be run via sbt.  They will also run at build time automatically.

```
sbt test
```
