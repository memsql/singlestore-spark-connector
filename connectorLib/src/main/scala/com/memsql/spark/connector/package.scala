package com.memsql.spark
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import scala.reflect.ClassTag

package object connector {
  implicit def toRDDFunctions(rdd: RDD[Row]): RDDFunctions =
    new RDDFunctions(rdd)

  implicit def toRDDFunctions[T: ClassTag](rdd: RDD[Array[T]]): RDDFunctionsLegacy[T] =
    new RDDFunctionsLegacy(rdd)

  implicit def toDataFrameFunctions(df: DataFrame): DataFrameFunctions = 
    new DataFrameFunctions(df)
}
