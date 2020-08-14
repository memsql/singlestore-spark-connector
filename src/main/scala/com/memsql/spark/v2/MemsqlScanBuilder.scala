package com.memsql.spark.v2

import com.memsql.spark.{JdbcHelpers, MemsqlOptions}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType

case class MemsqlScanBuilder(query: String,
                             options: MemsqlOptions,
                             @transient val sqlContext: SQLContext)
    extends ScanBuilder {
  override def build(): Scan = MemsqlScan(query, options, sqlContext)
}

class MemsqlPartition extends InputPartition

case class MemsqlScan(query: String, options: MemsqlOptions, @transient val sqlContext: SQLContext)
    extends Scan
    with Batch {

  lazy val schema = JdbcHelpers.loadSchema(options, query, Nil)

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new MemsqlPartition())
  }
  override def createReaderFactory(): PartitionReaderFactory = {
    new MemsqlPartitionReaderFactory(query, Nil, options, schema)
  }
}
