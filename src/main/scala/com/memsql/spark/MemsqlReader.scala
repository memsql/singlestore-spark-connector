package com.memsql.spark

import java.sql.SQLSyntaxErrorException

import com.memsql.spark.SQLGen.VariableList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression => CatalystExpression}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.sources.{BaseRelation, CatalystScan, TableScan}
import org.apache.spark.sql.{Row, SQLContext}

object MemsqlReader extends LazyLogging {
  def apply(opts: CaseInsensitiveMap[String], sqlContext: SQLContext): MemsqlReader = {
    MemsqlReader(MemsqlOptions.getQuery(opts), Nil, MemsqlOptions(opts), sqlContext)
  }
}

case class MemsqlReader(query: String,
                        variables: VariableList,
                        options: MemsqlOptions,
                        @transient val sqlContext: SQLContext,
                        isFinal: Boolean = false)
    extends BaseRelation
    with LazyLogging
    with TableScan
    with CatalystScan {

  override lazy val schema = JdbcHelpers.loadSchema(options, query, variables)

  override def buildScan(): RDD[Row] = {
    MemsqlRDD(query, variables, options, schema, sqlContext.sparkContext)
  }

  override def buildScan(rawColumns: Seq[Attribute],
                         rawFilters: Seq[CatalystExpression]): RDD[Row] = {
    // we don't have to push down *everything* using this interface since Spark will
    // run the projection and filter again upon receiving the results from MemSQL
    val projection = rawColumns.flatMap(ExpressionGen.apply.lift(_)).reduceOption(_ + "," + _)
    val filters    = rawFilters.flatMap(ExpressionGen.apply.lift(_)).reduceOption(_ + "AND" + _)

    val stmt = (projection, filters) match {
      case (Some(p), Some(f)) =>
        SQLGen
          .select(p)
          .from(SQLGen.Relation(Nil, this, SQLGen.aliasGen.next, null))
          .where(f)
      case (Some(p), None) =>
        SQLGen
          .select(p)
          .from(SQLGen.Relation(Nil, this, SQLGen.aliasGen.next, null))
      case (None, Some(f)) =>
        SQLGen.selectAll
          .from(SQLGen.Relation(Nil, this, SQLGen.aliasGen.next, null))
          .where(f)
      case _ =>
        return buildScan
    }

    val newReader = copy(query = stmt.sql, variables = stmt.variables)
    log.trace(s"CatalystScan additional rewrite:\n${newReader}")

    newReader.buildScan
  }

  override def toString: String = {
    val explain =
      try {
        JdbcHelpers.explainQuery(options, query, variables)
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
