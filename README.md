# MemSQL Spark Connector
## Version: 3.0.0-beta [![Build Status](https://travis-ci.com/memsql/memsql-spark-connector.svg?branch=3.0.0-beta)](https://travis-ci.com/memsql/memsql-spark-connector) [![License](http://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

> :warning: **This is a beta release**: [Go back to the stable branch](https://github.com/memsql/memsql-spark-connector)

## Major changes from the 2.0.0 connector

The MemSQL Spark Connector 3.0.0 has a number of key features and enhancements:

* Introduces SQL Optimization & Rewrite for most query shapes and compatible expressions
* Implemented as a native Spark SQL plugin
* Supports both the DataSource and DataSourceV2 API for maximum support of current and future functionality
* Contains deep integrations with the Catalyst query optimizer
* Is compatible with Spark 2.3
* Leverages MemSQL LOAD DATA to accelerate ingest from Spark via compression, vectorized cpu instructions, and optimized segment sizes
* Takes advantage of all the latest and greatest features in MemSQL 7.0
