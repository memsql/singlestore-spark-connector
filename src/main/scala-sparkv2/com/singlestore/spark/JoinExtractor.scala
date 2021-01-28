package com.singlestore.spark

import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.catalyst.expressions.Expression

case object JoinExtractor {
  def unapply(l: LogicalPlan): Option[(LogicalPlan, LogicalPlan, JoinType, Option[Expression])] = {
    l match {
      case Join(left, right, joinType, condition) => Some((left, right, joinType, condition))
      case _                                      => None
    }
  }
}
