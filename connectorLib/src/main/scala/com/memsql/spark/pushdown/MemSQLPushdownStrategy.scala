package com.memsql.spark.pushdown

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SQLContext, MemSQLRelation, MemSQLLogicalRelation, Strategy}

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
    try {
      buildQueryTree(plan) match {
        case Some(queryTree) => Seq(MemSQLPhysicalRDD.fromAbstractQueryTree(sparkContext, queryTree))
        case _ => Nil
      }
    } catch {
      // In the case that we fail to handle the plan we will raise MatchError.
      // Return Nil to let another strategy handle this tree.
      case _: MatchError => Nil
    }
  }

  def buildQueryTree(plan: LogicalPlan): Option[AbstractQuery] = plan match {
    case Filter(condition, child) =>
      for {
        subTree <- buildQueryTree(child)
      } yield PartialQuery(
        output=subTree.output,
        inner=subTree,
        suffix=Some(new SQLBuilder().raw(" WHERE ").addExpression(condition))
      )

    case Project(Nil, child) => buildQueryTree(child)
    case Project(fields, child) =>
      for {
        subTree <- buildQueryTree(child)
      } yield PartialQuery(
        output=fields.map(_.toAttribute),
        prefix=Some(new SQLBuilder().addExpressions(fields, ", ")),
        inner=subTree
      )

    case Aggregate(Nil, Nil, child) => buildQueryTree(child)
    case Aggregate(groups, fields, child) =>
      for {
        subTree <- buildQueryTree(child)
      } yield PartialQuery(
        output=fields.map(_.toAttribute),
        prefix=OptionSeq(fields).map { _ =>
          new SQLBuilder().addExpressions(fields, ", ")
        },
        inner=subTree,
        suffix=OptionSeq(groups).map { _ =>
          new SQLBuilder().raw(" GROUP BY ").addExpressions(groups, ", ")
        }
      )

    // NOTE: We can only push down global sorts into MemSQL, not per partition sorts.
    // If we need to implement per-partition sorts we can probably do it for certain
    // queries that stay local on the leaves
    case Sort(Nil, /* global= */ true, child) => buildQueryTree(child)
    case Sort(orderings, /* global= */ true, child) =>
      for {
        subTree <- buildQueryTree(child)
      } yield PartialQuery(
        output=subTree.output,
        inner=subTree,
        suffix=Some(new SQLBuilder().raw(" ORDER BY ").addExpressions(orderings, ", "))
      )

    case Join(left, right, Inner, condition) =>
      for {
        leftSubTree <- buildQueryTree(left)
        rightSubTree <- buildQueryTree(right)
        if leftSubTree.sharesCluster(rightSubTree)
      } yield {
        JoinQuery(
          output=leftSubTree.output ++ rightSubTree.output,
          condition=condition.map { c => new SQLBuilder().addExpression(c) },
          left=leftSubTree,
          right=rightSubTree
        )
      }

    case MemSQLLogicalRelation(r: MemSQLRelation) => Some(BaseQuery(r))

    case _ => None
  }
}

