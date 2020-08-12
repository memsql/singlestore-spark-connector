package com.memsql.spark.v2

import java.util

import com.memsql.spark.{JdbcHelpers, LazyLogging, MemsqlOptions}
import com.memsql.spark.SQLGen.{SQLGenContext, VariableList}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

case class MemsqlTable(options: MemsqlOptions,
                       @transient val sparkContext: SparkContext,
                       table: TableIdentifier,
                       userSchema: Option[StructType] = None,
                       isFinal: Boolean = false,
                       expectedOutput: Seq[Attribute] = Nil,
                       context: SQLGenContext)
    extends Table
    with SupportsRead
    with SupportsWrite
    with LazyLogging {

//  lazy val tableSchema = JdbcHelpers.loadSchema(options, query, variables)

  override def name(): String = table.table

  override def schema(): StructType = userSchema.getOrElse(inferSchema())

  private def inferSchema(): StructType = {}

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SimpleScanBuilder()

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val schema = info.schema()
    MemsqlLoadDataWriteBuilder(null, 0, 0, false, null, null, null)
  }
}
