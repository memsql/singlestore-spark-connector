package com.memsql.spark.v2

import java.util

import com.memsql.spark.{JdbcHelpers, LazyLogging, MemsqlOptions}
import com.memsql.spark.SQLGen.{SQLGenContext, VariableList}
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{
  StagedTable,
  SupportsRead,
  SupportsWrite,
  Table,
  TableCapability
}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

case class MemsqlTable(query: String,
                       options: MemsqlOptions,
                       @transient val sparkContext: SparkContext,
                       table: TableIdentifier,
                       userSchema: Option[StructType] = None,
                       isFinal: Boolean = false,
                       expectedOutput: Seq[Attribute] = Nil,
                       context: SQLGenContext)
    extends StagedTable
    with SupportsRead
    with SupportsWrite
    with LazyLogging {

  lazy val tableSchema = JdbcHelpers.loadSchema(options, query, Nil)

  override def name(): String = table.table

  override def schema(): StructType = userSchema.getOrElse(tableSchema)

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SimpleScanBuilder()

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val isReferenceTable = JdbcHelpers.isReferenceTable(options, table)
    MemsqlLoadDataWriteBuilder(info.schema(),
                               TaskContext.getPartitionId(),
                               0,
                               isReferenceTable,
                               SaveMode.Append,
                               table,
                               options)
  }

  override def commitStagedChanges(): Unit = {}

  override def abortStagedChanges(): Unit = {}
}
