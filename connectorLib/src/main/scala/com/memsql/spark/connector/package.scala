package com.memsql.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.memsql.SparkImplicits

package object connector {
  implicit class DataFrameFunctions(df: DataFrame) extends SparkImplicits.DataFrameFunctions(df)
}
