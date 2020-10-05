package com.memsql.spark

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object PlanToCommentSQL {
  val MAX_PLAN_FIELDS: Int            = Int.MaxValue
  def convert(p: LogicalPlan): String = { p.simpleString(MAX_PLAN_FIELDS).replace("\n", "\n-- ") }
}
