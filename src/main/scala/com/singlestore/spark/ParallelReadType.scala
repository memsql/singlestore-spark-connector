package com.singlestore.spark

sealed trait ParallelReadType

case object ReadFromLeaves                  extends ParallelReadType
case object ReadFromAggregators             extends ParallelReadType
case object ReadFromAggregatorsMaterialized extends ParallelReadType

object ParallelReadType {
  def apply(value: String): ParallelReadType = value.toLowerCase match {
    case "readfromleaves"                  => ReadFromLeaves
    case "readfromaggregators"             => ReadFromAggregators
    case "readfromaggregatorsmaterialized" => ReadFromAggregatorsMaterialized
    case _ =>
      throw new IllegalArgumentException(
        s"""Illegal argument for `${SinglestoreOptions.PARALLEL_READ_FEATURES}` option. Valid arguments are:
           | - "ReadFromLeaves"
           | - "ReadFromAggregators"
           | - "ReadFromAggregatorsMaterialized"""".stripMargin)
  }
}
