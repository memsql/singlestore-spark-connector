package com.memsql.spark.v2

import java.sql.SQLSyntaxErrorException
import java.util

import com.memsql.spark.SQLGen.{ExpressionExtractor, SQLGenContext, VariableList}
import com.memsql.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.connector.catalog.{
  StagedTable,
  SupportsRead,
  SupportsWrite,
  Table,
  TableCapability
}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.sources.{CatalystScan, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{SparkContext, TaskContext}

import scala.collection.JavaConverters._

case class MemsqlTable(query: String,
                       variables: VariableList,
                       memsqlOptions: MemsqlOptions,
                       @transient val sparkContext: SparkContext,
                       table: TableIdentifier,
                       userSchema: Option[StructType] = None,
                       isFinal: Boolean = false,
                       expectedOutput: Seq[Attribute] = Nil,
                       context: SQLGenContext)
    extends Table
    with SupportsRead
    with SupportsWrite
    with TableScan
    with CatalystScan
    with LazyLogging {

  override def buildScan(): RDD[Row] = {
    MemsqlRDD(query, Nil, memsqlOptions, tableSchema, expectedOutput, sparkContext)
  }

  override def buildScan(requiredColumns: Seq[Attribute], rawFilters: Seq[Expression]): RDD[Row] = {
    // we don't have to push down *everything* using this interface since Spark will
    // run the projection and filter again upon receiving the results from MemSQL
    val projection =
      requiredColumns
        .flatMap(ExpressionGen.apply(ExpressionExtractor(context)).lift(_))
        .reduceOption(_ + "," + _)
    val filters =
      rawFilters
        .flatMap(ExpressionGen.apply(ExpressionExtractor(context)).lift(_))
        .reduceOption(_ + "AND" + _)

    val stmt = (projection, filters) match {
      case (Some(p), Some(f)) =>
        SQLGen
          .select(p)
          .from(SQLGen.RelationV2(Nil, this, context.nextAlias(), null))
          .where(f)
          .output(requiredColumns)
      case (Some(p), None) =>
        SQLGen
          .select(p)
          .from(SQLGen.RelationV2(Nil, this, context.nextAlias(), null))
          .output(requiredColumns)
      case (None, Some(f)) =>
        SQLGen.selectAll
          .from(SQLGen.RelationV2(Nil, this, context.nextAlias(), null))
          .where(f)
          .output(expectedOutput)
      case _ =>
        return buildScan
    }

    val newReader = copy(query = stmt.sql, variables = stmt.variables, expectedOutput = stmt.output)

    if (log.isTraceEnabled) {
      log.trace(s"CatalystScan additional rewrite:\n${newReader}")
    }

    newReader.buildScan
  }

  lazy val tableSchema = JdbcHelpers.loadSchema(memsqlOptions, query, Nil)

  override def name(): String = table.table

  override def schema(): StructType = userSchema.getOrElse(tableSchema)

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ,
        TableCapability.BATCH_WRITE,
        TableCapability.ACCEPT_ANY_SCHEMA,
        TableCapability.TRUNCATE).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    MemsqlScanBuilder(query, memsqlOptions, SparkSession.active.sqlContext)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val isReferenceTable = JdbcHelpers.isReferenceTable(memsqlOptions, table)
    MemsqlLoadDataWriteBuilder(info.schema(),
                               TaskContext.getPartitionId(),
                               0,
                               isReferenceTable,
                               SaveMode.Append,
                               table,
                               memsqlOptions)
  }

  override def toString: String = {
    val explain =
      try {
        JdbcHelpers.explainQuery(memsqlOptions, query, variables)
      } catch {
        case e: SQLSyntaxErrorException => e.toString
        case e: Exception               => throw e
      }
    val v = variables.map(_.variable).mkString(", ")

    s"""
       |---------------
       |MemSQL Query
       |Variables: ($v)
       |SQL:
       |$query
       |
       |EXPLAIN:
       |$explain
       |---------------
      """.stripMargin
  }
}
