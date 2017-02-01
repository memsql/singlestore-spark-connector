MemSQL Spark 2.0 Connector
====================
The MemSQL 2.0 Spark connector enables users to load data from MemSQL tables into Spark Dataframes, and write Spark Dataframes to MemSQL tables.


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
libraryDependencies  += "com.memsql" %% "memsql-connector" % "2.0.0"
```

or Maven:

```xml
<dependency>
    <groupId>com.memsql</groupId>
    <artifactId>memsql-connector_2.11</artifactId>
    <version>2.0.0</version>
</dependency>
```


Usage
-----

MemSQL Spark Connector leverages Spark SQL's Data Sources API. The connection to MemSQL requires setting the following Spark configuration settings.

| Setting name             | Default value if not specified |
| --------------------     | ------------------------------ |
| spark.memsql.host        | localhost                      |
| spark.memsql.port        | 3306                           |
| spark.memsql.password    | None                           |


### Loading data from MemSQL

The following example creates a Dataframe from the table "illinois" in the database "customers". To use the library, pass in "com.memsql.spark.connector" as the `format` parameter so Spark will call the MemSQL Spark Connector code. The option `path` is the full path of the table using the syntax "$database_name.$table_name".

```scala
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

val conf = new SparkConf().setAppName("MemSQL Spark Connector Example").set("spark.memsql.host", "10.0.0.190").set("spark.memsql.password", "foobar")
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

Instead of specifying a MemSQL table as the `path` in the options, the user can opt to create a DataFrame from a SQL query with the option `query`. This can minimize the amount of data transferred from MemSQL to Spark, and push down distributed computations to MemSQL instead of Spark.

```scala
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

val conf = new SparkConf().setAppName("MemSQL Spark Connector Example").set("spark.memsql.host", "10.0.0.190").set("spark.memsql.password", "foobar")
val spark = SparkSession.builder().config(conf).getOrCreate()

val customersFromIllinois = spark
	.read
	.format("com.memsql.spark.connector")
	.options(Map("query" -> ("select age_group, count(*) from customers.illinois where number_of_orders > 3 GROUP BY age_group")))
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

The `mode` specifies how to handle existing data if present. The default, if unspecified, is "error", which means that if data already exists in people.students, an error is to be thrown.


Building and Testing
--------------------

You can use SBT to compile the library

```
sbt build
```

All unit tests can be run via sbt.  They will also run at build time automatically.

```
sbt test
```
