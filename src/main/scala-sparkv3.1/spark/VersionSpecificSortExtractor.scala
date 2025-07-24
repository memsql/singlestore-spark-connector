package com.singlestore.spark

import org.apache.spark.sql.catalyst.expressions.{Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.execution.datasources.LogicalRelation

object VersionSpecificSortExtractor {
  def unapply(plan: LogicalPlan): Option[(Seq[SortOrder], Boolean, LogicalPlan)] =
    plan match {
      case s @ Sort(order, global, child) =>
        Option(order, global, child)
      case _ => None
    }
}
