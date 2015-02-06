package com.memsql.spark

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

package object connector {
  implicit def toRDDFunctions[T : ClassTag](rdd: RDD[Array[T]]): RDDFunctions[T] =
    new RDDFunctions[T](rdd)
}
