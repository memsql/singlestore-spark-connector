package com.singlestore.spark

import java.sql.SQLSyntaxErrorException

import com.singlestore.spark.SQLGen.{ExpressionExtractor, SQLGenContext, VariableList}
import org.apache.spark.DataSourceTelemetryHelpers
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression => CatalystExpression}
import org.apache.spark.sql.sources.{BaseRelation, CatalystScan, TableScan}
import org.apache.spark.sql.{Row, SQLContext}

import scala.util.Random

case class SinglestoreReaderNoPushdown(query: String,
                                       options: SinglestoreOptions,
                                       @transient val sqlContext: SQLContext)
    extends BaseRelation
    with TableScan {

  override lazy val schema = JdbcHelpers.loadSchema(options, query, Nil)

  override def buildScan: RDD[Row] = {
    val randHex = Random.nextInt().toHexString
    val rdd =
      SinglestoreRDD(
        query,
        Nil,
        options,
        schema,
        Nil,
        resultMustBeSorted = false,
        schema
          .filter(sf => options.parallelReadRepartitionColumns.contains(sf.name))
          .map(sf => SQLGen.Ident(sf.name).sql),
        sqlContext.sparkContext,
        randHex,
        DataSourceTelemetryHelpers.createDataSourceTelemetry(
          sqlContext.sparkContext,
          Some("SinglestoreReaderNoPushdown")
        )
      )
      // Add random hex to the name
      // It is needed to generate unique names for result tables during parallel read
      .setName("SingleStoreRDD" + randHex)
    if (rdd.parallelReadType.contains(ReadFromAggregators)) {
      // Wrap an RDD with barrier stage, to force all readers start reading at the same time.
      // Repartition it to force spark to read data and do all other computations in different stages.
      // Otherwise we will likely get the following error:
      // [SPARK-24820][SPARK-24821]: Barrier execution mode does not allow the following
      // pattern of RDD chain within a barrier stage...
      rdd.barrier().mapPartitions(v => v).repartition(rdd.getNumPartitions)
    } else {
      rdd
    }
  }
}

case class SinglestoreReader(query: String,
                             variables: VariableList,
                             options: SinglestoreOptions,
                             @transient val sqlContext: SQLContext,
                             isFinal: Boolean = false,
                             expectedOutput: Seq[Attribute] = Nil,
                             var resultMustBeSorted: Boolean = false,
                             context: SQLGenContext)
    extends BaseRelation
    with LazyLogging
    with TableScan
    with CatalystScan
    with DataSourceTelemetryHelpers {

  override lazy val schema = JdbcHelpers.loadSchema(options, query, variables)

  override def buildScan: RDD[Row] = {
    val randHex = Random.nextInt().toHexString
    val rdd =
      SinglestoreRDD(
        query,
        variables,
        options,
        schema,
        expectedOutput,
        resultMustBeSorted,
        expectedOutput
          .filter(attr => options.parallelReadRepartitionColumns.contains(attr.name))
          .map(attr => context.ident(attr.name, attr.exprId)),
        sqlContext.sparkContext,
        randHex,
        DataSourceTelemetryHelpers.createDataSourceTelemetry(
          sqlContext.sparkContext,
          Some("SinglestoreReader")
        )
      )     
      // Add random hex to the name
      // It is needed to generate unique names for result tables during parallel read
      .setName("SingleStoreRDD" + randHex)
    if (rdd.parallelReadType.contains(ReadFromAggregators)) {
      // Wrap an RDD with barrier stage, to force all readers start reading at the same time.
      // Repartition it to force spark to read data and do all other computations in different stages.
      // Otherwise we will likely get the following error:
      // [SPARK-24820][SPARK-24821]: Barrier execution mode does not allow the following
      // pattern of RDD chain within a barrier stage...
      rdd.barrier().mapPartitions(v => v).repartition(rdd.getNumPartitions)
    } else {
      rdd
    }
  }

  override def buildScan(rawColumns: Seq[Attribute],
                         rawFilters: Seq[CatalystExpression]): RDD[Row] = {
    // we don't have to push down *everything* using this interface since Spark will
    // run the projection and filter again upon receiving the results from SingleStore
    val projection =
      rawColumns
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
          .from(SQLGen.Relation(Nil, this, context.nextAlias(), null))
          .where(f)
          .output(rawColumns)
      case (Some(p), None) =>
        SQLGen
          .select(p)
          .from(SQLGen.Relation(Nil, this, context.nextAlias(), null))
          .output(rawColumns)
      case (None, Some(f)) =>
        SQLGen.selectAll
          .from(SQLGen.Relation(Nil, this, context.nextAlias(), null))
          .where(f)
          .output(expectedOutput)
      case _ =>
        return buildScan
    }

    val newReader = copy(query = stmt.sql, variables = stmt.variables, expectedOutput = stmt.output)

    log.info(logEventNameTagger(s"CatalystScan additional rewrite:\n$newReader"))

    newReader.buildScan
  }

  override def toString: String = {
    val explain = if (log.isTraceEnabled) {
      val explainStr = try {
        JdbcHelpers.explainQuery(options, query, variables)
      } catch {
        case e: SQLSyntaxErrorException => e.toString
        case e: Exception               => throw e
      }
      s"""
         |\nEXPLAIN:
         |$explainStr
         """.stripMargin
    } else { "" }
    val v = variables.map(_.variable).mkString(", ")

    s"""
      |---------------
      |SingleStore Query
      |Variables: ($v)
      |SQL:
      |${JdbcHelpers.appendTagsToQuery(options, query)}$explain
      |---------------
      """.stripMargin
  }
}
