package com.singlestore.spark

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object PlanToCommentSQL {
  def convert(p: LogicalPlan): String = { p.simpleString.replace("\n", "\n-- ") }
}
