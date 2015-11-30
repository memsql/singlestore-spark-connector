package com.memsql.spark.pushdown

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.memsql.{MemSQLRelationUtils, MemSQLRelation}
import org.apache.spark.sql.{SQLContext, Strategy}

object MemSQLPushdownStrategy {
  /**
   * In Spark 1.3 - Spark 1.5 extraStrategies can modify a plan before
   * any other strategy (including DataSourceStrategy)
   *
   * If you want to execute MemSQL queries with a non-MemSQL SQLContext,
   * use this method to enable pushdown on your SQLContext.
   */
  def patchSQLContext(s: SQLContext): Unit = {
    val strategies = s.experimental.extraStrategies
    if (!strategies.exists(s => s.isInstanceOf[MemSQLPushdownStrategy])) {
      s.experimental.extraStrategies ++= Seq(new MemSQLPushdownStrategy(s.sparkContext))
    }
  }

  /**
   * Remove MemSQLPushdownStrategy from the specified SQLContext
   */
  def unpatchSQLContext(s: SQLContext): Unit = {
    s.experimental.extraStrategies = s.experimental.extraStrategies
      .filterNot(s => s.isInstanceOf[MemSQLPushdownStrategy])
  }
}

class MemSQLPushdownStrategy(sparkContext: SparkContext) extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val fieldIdIter = Iterator.from(1).map(n => s"field$n")

    try {
      val alias = QueryAlias("pushdown")
      buildQueryTree(fieldIdIter, alias, plan) match {
        case Some(queryTree) => Seq(MemSQLPhysicalRDD.fromAbstractQueryTree(sparkContext, queryTree))
        case _ => Nil
      }
    } catch {
      // In the case that we fail to handle the plan we will raise MatchError.
      // Return Nil to let another strategy handle this tree.
      case _: MatchError => Nil
    }
  }

  def buildQueryTree(fieldIdIter: Iterator[String], alias: QueryAlias, plan: LogicalPlan): Option[AbstractQuery] = plan match {
    case Filter(condition, child) =>
      for {
        subTree <- buildQueryTree(fieldIdIter, alias.child, child)
      } yield PartialQuery(
        alias=alias,
        output=subTree.output,
        inner=subTree,
        suffix=Some(SQLBuilder
          .withFields(subTree.qualifiedOutput)
          .raw(" WHERE ").addExpression(condition)
        )
      )

    case Project(fields, child) =>
      for {
        subTree <- buildQueryTree(fieldIdIter, alias.child, child)
        expressions = renameExpressions(fieldIdIter, fields)
      } yield PartialQuery(
        alias=alias,
        output=expressions.map(_.toAttribute),
        prefix=SQLBuilder
          .withFields(subTree.qualifiedOutput)
          .maybeAddExpressions(expressions, ", "),
        inner=subTree
      )

    case Aggregate(groups, fields, child) =>
      for {
        subTree <- buildQueryTree(fieldIdIter, alias.child, child)
        expressions = renameExpressions(fieldIdIter, fields)
      } yield PartialQuery(
        alias=alias,
        output=expressions.map(_.toAttribute),
        prefix=SQLBuilder
          .withFields(subTree.qualifiedOutput)
          .maybeAddExpressions(expressions, ", "),
        inner=subTree,
        suffix=SQLBuilder
          .withFields(subTree.qualifiedOutput)
          .raw(" GROUP BY ").maybeAddExpressions(groups, ", ")
      )

    // NOTE: We can only push down global sorts into MemSQL, not per partition sorts.
    // If we need to implement per-partition sorts we can probably do it for certain
    // queries that stay local on the leaves
    case Sort(orderings, /* global= */ true, child) =>
      for {
        subTree <- buildQueryTree(fieldIdIter, alias.child, child)
      } yield PartialQuery(
        alias=alias,
        output=subTree.output,
        inner=subTree,
        suffix=SQLBuilder
          .withFields(subTree.qualifiedOutput)
          .raw(" ORDER BY ").maybeAddExpressions(orderings, ", ")
      )

    // NOTE: We ignore Subquerys when we are doing query pushdown
    // since we rewrite all of the qualifiers explicitly
    case Subquery(_, child) =>
      for {
        subTree <- buildQueryTree(fieldIdIter, alias.child, child)
      } yield subTree

    case Join(left, right, Inner, condition) => {
      val (leftAlias, rightAlias) = alias.fork
      for {
        leftSubTree <- buildQueryTree(fieldIdIter, leftAlias.child, left)
        rightSubTree <- buildQueryTree(fieldIdIter, rightAlias.child, right)
        if leftSubTree.sharesCluster(rightSubTree)
        qualifiedOutput = leftSubTree.qualifiedOutput ++ rightSubTree.qualifiedOutput
      } yield {
        JoinQuery(
          alias=alias,
          output=leftSubTree.output ++ rightSubTree.output,
          condition=condition.map { c =>
            SQLBuilder
              .withFields(qualifiedOutput)
              .addExpression(c)
          },
          left=leftSubTree,
          right=rightSubTree
        )
      }
    }

    case MemSQLRelationUtils(r: MemSQLRelation) => Some(BaseQuery(alias, r))

    case _ => None
  }

  /**
    * Assign field names to each expression for a given alias
    */
  def renameExpressions(fieldIdIter: Iterator[String], expressions: Seq[NamedExpression]): Seq[NamedExpression] =
    expressions.map {
      // We need to special case Alias, since this is not valid SQL:
      // select (foo as bar) as baz from ...
      case a @ Alias(child: Expression, name: String) => {
        Alias(child, fieldIdIter.next)(a.exprId, Nil, a.explicitMetadata)
      }
      case expr: NamedExpression => {
        Alias(expr, fieldIdIter.next)(expr.exprId, Nil, None)
      }
    }
}

