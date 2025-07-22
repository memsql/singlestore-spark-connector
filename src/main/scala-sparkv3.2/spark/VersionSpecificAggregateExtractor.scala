package com.singlestore.spark

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}

object VersionSpecificAggregateExtractor {
  def unapply(plan: LogicalPlan): Option[(Seq[Expression], Seq[NamedExpression], LogicalPlan)] =
    plan match {
      case a @ Aggregate(groupingExpressions, aggregateExpressions, child) =>
        Option(groupingExpressions, aggregateExpressions, child)
      case _ => None
    }
}
