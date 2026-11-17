package com.singlestore.spark

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}

object VersionSpecificSortExtractor {
  def unapply(plan: LogicalPlan): Option[(Seq[SortOrder], Boolean, LogicalPlan)] =
    plan match {
      case s @ Sort(order, global, child, _) =>
        Option(order, global, child)
      case _ => None
    }
}
