package com.singlestore.spark

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Window}

object VersionSpecificWindowExtractor {
  def unapply(plan: LogicalPlan)
    : Option[(Seq[NamedExpression], Seq[Expression], Seq[SortOrder], LogicalPlan)] =
    plan match {
      case Window(windowExpressions, partitionSpec, orderSpec, child, _) =>
        Option(windowExpressions, partitionSpec, orderSpec, child)
      case _ => None
    }
}
