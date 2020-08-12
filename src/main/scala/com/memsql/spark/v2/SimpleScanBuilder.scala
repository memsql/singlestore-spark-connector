package com.memsql.spark.v2

import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReaderFactory,
  Scan,
  ScanBuilder
}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class SimpleScanBuilder extends ScanBuilder {
  override def build(): Scan = new SimpleScan
}

class SimplePartition extends InputPartition

class SimpleScan extends Scan with Batch {
  override def readSchema(): StructType = StructType(Array(StructField("value", StringType)))

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new SimplePartition())
  }
  override def createReaderFactory(): PartitionReaderFactory = {
    null
//    new SimplePartitionReaderFactory()
  }
}
